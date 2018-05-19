package ast

import (
	"fmt"
)

type Node interface {
	Accept(Visitor) Node

	Pos() Pos

	Type(Scope) (Type, error)
}

type Pos struct {
	Column, Line int // Column/Line number, starting at 1
}

func (p Pos) String() string {
	return fmt.Sprintf("%d:%d", p.Line, p.Column)
}

//
// should be returned. We build this replacement directly into the visitor
// building it into the AST itself makes it required for future Node
//
type Visitor func(Node) Node

type Type uint32

const (
	TypeInvalid Type = 0
	TypeAny     Type = 1 << iota
	TypeString
	TypeInt
	TypeFloat
	TypeList
	TypeMap
)

func (t Type) Printable() string {
	switch t {
	case TypeInvalid:
		return "invalid type"
	case TypeAny:
		return "any type"
	case TypeString:
		return "type string"
	case TypeInt:
		return "type int"
	case TypeFloat:
		return "type float"
	case TypeList:
		return "type list"
	case TypeMap:
		return "type map"
	default:
		return "unknown type"
	}
}
