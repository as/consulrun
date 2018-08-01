package hcl

import (
	"fmt"

	"github.com/as/consulrun/hashicorp/hcl/hcl/ast"
	hclParser "github.com/as/consulrun/hashicorp/hcl/hcl/parser"
	jsonParser "github.com/as/consulrun/hashicorp/hcl/json/parser"
)

//
func ParseBytes(in []byte) (*ast.File, error) {
	return parse(in)
}

func ParseString(input string) (*ast.File, error) {
	return parse([]byte(input))
}

func parse(in []byte) (*ast.File, error) {
	switch lexMode(in) {
	case lexModeHcl:
		return hclParser.Parse(in)
	case lexModeJson:
		return jsonParser.Parse(in)
	}

	return nil, fmt.Errorf("unknown config format")
}

//
func Parse(input string) (*ast.File, error) {
	return parse([]byte(input))
}
