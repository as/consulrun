package hil

import (
	"sync"

	"github.com/as/consulrun/hashicorp/hil/ast"
)

var parserLock sync.Mutex
var parserResult ast.Node

func Parse(v string) (ast.Node, error) {

	parserLock.Lock()
	defer parserLock.Unlock()

	parserResult = nil

	lex := &parserLex{Input: v}

	parserParse(lex)

	return parserResult, lex.Err
}
