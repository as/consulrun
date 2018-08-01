package iradix

type rawIterator struct {
	node *Node

	stack []rawStackEntry

	pos *Node

	path string
}

type rawStackEntry struct {
	path  string
	edges edges
}

func (i *rawIterator) Front() *Node {
	return i.pos
}

func (i *rawIterator) Path() string {
	return i.path
}

func (i *rawIterator) Next() {

	if i.stack == nil && i.node != nil {
		i.stack = []rawStackEntry{
			{
				edges: edges{
					edge{node: i.node},
				},
			},
		}
	}

	for len(i.stack) > 0 {

		n := len(i.stack)
		last := i.stack[n-1]
		elem := last.edges[0].node

		if len(last.edges) > 1 {
			i.stack[n-1].edges = last.edges[1:]
		} else {
			i.stack = i.stack[:n-1]
		}

		if len(elem.edges) > 0 {
			path := last.path + string(elem.prefix)
			i.stack = append(i.stack, rawStackEntry{path, elem.edges})
		}

		i.pos = elem
		i.path = last.path + string(elem.prefix)
		return
	}

	i.pos = nil
	i.path = ""
}
