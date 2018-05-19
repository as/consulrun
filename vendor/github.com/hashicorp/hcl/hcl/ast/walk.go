package ast

import "fmt"

type WalkFunc func(Node) (Node, bool)

func Walk(node Node, fn WalkFunc) Node {
	rewritten, ok := fn(node)
	if !ok {
		return rewritten
	}

	switch n := node.(type) {
	case *File:
		n.Node = Walk(n.Node, fn)
	case *ObjectList:
		for i, item := range n.Items {
			n.Items[i] = Walk(item, fn).(*ObjectItem)
		}
	case *ObjectKey:

	case *ObjectItem:
		for i, k := range n.Keys {
			n.Keys[i] = Walk(k, fn).(*ObjectKey)
		}

		if n.Val != nil {
			n.Val = Walk(n.Val, fn)
		}
	case *LiteralType:

	case *ListType:
		for i, l := range n.List {
			n.List[i] = Walk(l, fn)
		}
	case *ObjectType:
		n.List = Walk(n.List, fn).(*ObjectList)
	default:

		fmt.Printf("unknown type: %T\n", n)
	}

	fn(nil)
	return rewritten
}
