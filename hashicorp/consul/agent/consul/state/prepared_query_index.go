package state

import (
	"fmt"
	"strings"

	"github.com/as/consulrun/hashicorp/consul/agent/consul/prepared_query"
)

type PreparedQueryIndex struct {
}

func (*PreparedQueryIndex) FromObject(obj interface{}) (bool, []byte, error) {
	wrapped, ok := obj.(*queryWrapper)
	if !ok {
		return false, nil, fmt.Errorf("invalid object given to index as prepared query")
	}

	query := toPreparedQuery(wrapped)
	if !prepared_query.IsTemplate(query) {
		return false, nil, nil
	}

	out := "\x00" + strings.ToLower(query.Name)
	return true, []byte(out), nil
}

func (p *PreparedQueryIndex) FromArgs(args ...interface{}) ([]byte, error) {
	return p.PrefixFromArgs(args...)
}

func (*PreparedQueryIndex) PrefixFromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("must provide only a single argument")
	}
	arg, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("argument must be a string: %#v", args[0])
	}
	arg = "\x00" + strings.ToLower(arg)
	return []byte(arg), nil
}
