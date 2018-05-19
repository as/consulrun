package prepared_query

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/hil"
	"github.com/hashicorp/hil/ast"
	"github.com/mitchellh/copystructure"
)

func IsTemplate(query *structs.PreparedQuery) bool {
	return query.Template.Type != ""
}

type CompiledTemplate struct {
	query *structs.PreparedQuery

	trees map[string]ast.Node

	re *regexp.Regexp

	removeEmptyTags bool
}

func Compile(query *structs.PreparedQuery) (*CompiledTemplate, error) {

	if query.Template.Type != structs.QueryTemplateTypeNamePrefixMatch {
		return nil, fmt.Errorf("Bad Template.Type '%s'", query.Template.Type)
	}

	ct := &CompiledTemplate{
		trees:           make(map[string]ast.Node),
		removeEmptyTags: query.Template.RemoveEmptyTags,
	}

	dup, err := copystructure.Copy(query)
	if err != nil {
		return nil, err
	}
	var ok bool
	ct.query, ok = dup.(*structs.PreparedQuery)
	if !ok {
		return nil, fmt.Errorf("Failed to copy query")
	}

	parse := func(path string, v reflect.Value) error {
		tree, err := hil.Parse(v.String())
		if err != nil {
			return fmt.Errorf("Bad format '%s' in Service%s: %s", v.String(), path, err)
		}

		ct.trees[path] = tree
		return nil
	}
	if err := walk(&ct.query.Service, parse); err != nil {
		return nil, err
	}

	if ct.query.Template.Regexp != "" {
		var err error
		ct.re, err = regexp.Compile(ct.query.Template.Regexp)
		if err != nil {
			return nil, fmt.Errorf("Bad Regexp: %s", err)
		}
	}

	if _, err = ct.Render(ct.query.Name, structs.QuerySource{}); err != nil {
		return nil, err
	}

	return ct, nil
}

func (ct *CompiledTemplate) Render(name string, source structs.QuerySource) (*structs.PreparedQuery, error) {

	if ct == nil {
		return nil, fmt.Errorf("Cannot render an uncompiled template")
	}

	dup, err := copystructure.Copy(ct.query)
	if err != nil {
		return nil, err
	}
	query, ok := dup.(*structs.PreparedQuery)
	if !ok {
		return nil, fmt.Errorf("Failed to copy query")
	}

	var matches []string
	if ct.re != nil {
		re := ct.re.Copy()
		matches = re.FindStringSubmatch(name)
	}

	match := ast.Function{
		ArgTypes:   []ast.Type{ast.TypeInt},
		ReturnType: ast.TypeString,
		Variadic:   false,
		Callback: func(inputs []interface{}) (interface{}, error) {
			i, ok := inputs[0].(int)
			if ok && i >= 0 && i < len(matches) {
				return matches[i], nil
			}
			return "", nil
		},
	}

	config := &hil.EvalConfig{
		GlobalScope: &ast.BasicScope{
			VarMap: map[string]ast.Variable{
				"name.full": {
					Type:  ast.TypeString,
					Value: name,
				},
				"name.prefix": {
					Type:  ast.TypeString,
					Value: query.Name,
				},
				"name.suffix": {
					Type:  ast.TypeString,
					Value: strings.TrimPrefix(name, query.Name),
				},
				"agent.segment": {
					Type:  ast.TypeString,
					Value: source.Segment,
				},
			},
			FuncMap: map[string]ast.Function{
				"match": match,
			},
		},
	}

	eval := func(path string, v reflect.Value) error {
		tree, ok := ct.trees[path]
		if !ok {
			return nil
		}

		res, err := hil.Eval(tree, config)
		if err != nil {
			return fmt.Errorf("Bad evaluation for '%s' in Service%s: %s", v.String(), path, err)
		}
		if res.Type != hil.TypeString {
			return fmt.Errorf("Expected Service%s field to be a string, got %s", path, res.Type)
		}

		v.SetString(res.Value.(string))
		return nil
	}
	if err := walk(&query.Service, eval); err != nil {
		return nil, err
	}

	if ct.removeEmptyTags {
		tags := make([]string, 0, len(query.Service.Tags))
		for _, tag := range query.Service.Tags {
			if tag != "" {
				tags = append(tags, tag)
			}
		}
		query.Service.Tags = tags
	}

	return query, nil
}
