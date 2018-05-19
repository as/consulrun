package parser

import "github.com/hashicorp/hcl/hcl/ast"

func flattenObjects(node ast.Node) {
	ast.Walk(node, func(n ast.Node) (ast.Node, bool) {

		list, ok := n.(*ast.ObjectList)
		if !ok {
			return n, true
		}

		items := make([]*ast.ObjectItem, 0, len(list.Items))
		frontier := make([]*ast.ObjectItem, len(list.Items))
		copy(frontier, list.Items)
		for len(frontier) > 0 {

			n := len(frontier)
			item := frontier[n-1]
			frontier = frontier[:n-1]

			switch v := item.Val.(type) {
			case *ast.ObjectType:
				items, frontier = flattenObjectType(v, item, items, frontier)
			case *ast.ListType:
				items, frontier = flattenListType(v, item, items, frontier)
			default:
				items = append(items, item)
			}
		}

		for i := len(items)/2 - 1; i >= 0; i-- {
			opp := len(items) - 1 - i
			items[i], items[opp] = items[opp], items[i]
		}

		list.Items = items
		return n, true
	})
}

func flattenListType(
	ot *ast.ListType,
	item *ast.ObjectItem,
	items []*ast.ObjectItem,
	frontier []*ast.ObjectItem) ([]*ast.ObjectItem, []*ast.ObjectItem) {

	for _, subitem := range ot.List {
		if _, ok := subitem.(*ast.ObjectType); !ok {
			items = append(items, item)
			return items, frontier
		}
	}

	for _, elem := range ot.List {

		frontier = append(frontier, &ast.ObjectItem{
			Keys:        item.Keys,
			Assign:      item.Assign,
			Val:         elem,
			LeadComment: item.LeadComment,
			LineComment: item.LineComment,
		})
	}

	return items, frontier
}

func flattenObjectType(
	ot *ast.ObjectType,
	item *ast.ObjectItem,
	items []*ast.ObjectItem,
	frontier []*ast.ObjectItem) ([]*ast.ObjectItem, []*ast.ObjectItem) {

	if ot.List.Items == nil {
		items = append(items, item)
		return items, frontier
	}

	for _, subitem := range ot.List.Items {
		if _, ok := subitem.Val.(*ast.ObjectType); !ok {
			items = append(items, item)
			return items, frontier
		}
	}

	for _, subitem := range ot.List.Items {

		keys := make([]*ast.ObjectKey, len(item.Keys)+len(subitem.Keys))
		copy(keys, item.Keys)
		copy(keys[len(item.Keys):], subitem.Keys)

		frontier = append(frontier, &ast.ObjectItem{
			Keys:        keys,
			Assign:      item.Assign,
			Val:         subitem.Val,
			LeadComment: item.LeadComment,
			LineComment: item.LineComment,
		})
	}

	return items, frontier
}
