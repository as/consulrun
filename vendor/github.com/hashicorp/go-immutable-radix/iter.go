package iradix

import "bytes"

type Iterator struct {
	node  *Node
	stack []edges
}

func (i *Iterator) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {

	i.stack = nil
	n := i.node
	watch = n.mutateCh
	search := prefix
	for {

		if len(search) == 0 {
			i.node = n
			return
		}

		_, n = n.getEdge(search[0])
		if n == nil {
			i.node = nil
			return
		}

		watch = n.mutateCh

		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]

		} else if bytes.HasPrefix(n.prefix, search) {
			i.node = n
			return
		} else {
			i.node = nil
			return
		}
	}
}

func (i *Iterator) SeekPrefix(prefix []byte) {
	i.SeekPrefixWatch(prefix)
}

func (i *Iterator) Next() ([]byte, interface{}, bool) {

	if i.stack == nil && i.node != nil {
		i.stack = []edges{
			{
				edge{node: i.node},
			},
		}
	}

	for len(i.stack) > 0 {

		n := len(i.stack)
		last := i.stack[n-1]
		elem := last[0].node

		if len(last) > 1 {
			i.stack[n-1] = last[1:]
		} else {
			i.stack = i.stack[:n-1]
		}

		if len(elem.edges) > 0 {
			i.stack = append(i.stack, elem.edges)
		}

		if elem.leaf != nil {
			return elem.leaf.key, elem.leaf.val, true
		}
	}
	return nil, nil, false
}
