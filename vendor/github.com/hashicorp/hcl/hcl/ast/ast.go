// Package ast declares the types used to represent syntax trees for HCL
package ast

import (
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/hcl/token"
)

type Node interface {
	node()
	Pos() token.Pos
}

func (File) node()         {}
func (ObjectList) node()   {}
func (ObjectKey) node()    {}
func (ObjectItem) node()   {}
func (Comment) node()      {}
func (CommentGroup) node() {}
func (ObjectType) node()   {}
func (LiteralType) node()  {}
func (ListType) node()     {}

type File struct {
	Node     Node            // usually a *ObjectList
	Comments []*CommentGroup // list of all comments in the source
}

func (f *File) Pos() token.Pos {
	return f.Node.Pos()
}

type ObjectList struct {
	Items []*ObjectItem
}

func (o *ObjectList) Add(item *ObjectItem) {
	o.Items = append(o.Items, item)
}

//
//
func (o *ObjectList) Filter(keys ...string) *ObjectList {
	var result ObjectList
	for _, item := range o.Items {

		if len(item.Keys) < len(keys) {
			continue
		}

		match := true
		for i, k := range item.Keys[:len(keys)] {
			keyi := keys[i]
			key := k.Token.Value().(string)
			if key != keyi && !strings.EqualFold(key, keyi) {
				match = false
				break
			}
		}
		if !match {
			continue
		}

		newItem := *item
		newItem.Keys = newItem.Keys[len(keys):]
		result.Add(&newItem)
	}

	return &result
}

func (o *ObjectList) Children() *ObjectList {
	var result ObjectList
	for _, item := range o.Items {
		if len(item.Keys) > 0 {
			result.Add(item)
		}
	}

	return &result
}

func (o *ObjectList) Elem() *ObjectList {
	var result ObjectList
	for _, item := range o.Items {
		if len(item.Keys) == 0 {
			result.Add(item)
		}
	}

	return &result
}

func (o *ObjectList) Pos() token.Pos {

	return o.Items[0].Pos()
}

type ObjectItem struct {
	Keys []*ObjectKey

	Assign token.Pos

	Val Node

	LeadComment *CommentGroup // associated lead comment
	LineComment *CommentGroup // associated line comment
}

func (o *ObjectItem) Pos() token.Pos {

	if len(o.Keys) == 0 {
		return token.Pos{}
	}

	return o.Keys[0].Pos()
}

type ObjectKey struct {
	Token token.Token
}

func (o *ObjectKey) Pos() token.Pos {
	return o.Token.Pos
}

type LiteralType struct {
	Token token.Token

	LineComment *CommentGroup
}

func (l *LiteralType) Pos() token.Pos {
	return l.Token.Pos
}

type ListType struct {
	Lbrack token.Pos // position of "["
	Rbrack token.Pos // position of "]"
	List   []Node    // the elements in lexical order
}

func (l *ListType) Pos() token.Pos {
	return l.Lbrack
}

func (l *ListType) Add(node Node) {
	l.List = append(l.List, node)
}

type ObjectType struct {
	Lbrace token.Pos   // position of "{"
	Rbrace token.Pos   // position of "}"
	List   *ObjectList // the nodes in lexical order
}

func (o *ObjectType) Pos() token.Pos {
	return o.Lbrace
}

type Comment struct {
	Start token.Pos // position of / or #
	Text  string
}

func (c *Comment) Pos() token.Pos {
	return c.Start
}

type CommentGroup struct {
	List []*Comment // len(List) > 0
}

func (c *CommentGroup) Pos() token.Pos {
	return c.List[0].Pos()
}

func (o *ObjectKey) GoString() string { return fmt.Sprintf("*%#v", *o) }
