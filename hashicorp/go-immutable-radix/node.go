package iradix

import (
	"bytes"
	"sort"
)

type WalkFn func(k []byte, v interface{}) bool

type leafNode struct {
	mutateCh chan struct{}
	key      []byte
	val      interface{}
}

type edge struct {
	label byte
	node  *Node
}

type Node struct {
	mutateCh chan struct{}

	leaf *leafNode

	prefix []byte

	edges edges
}

func (n *Node) isLeaf() bool {
	return n.leaf != nil
}

func (n *Node) addEdge(e edge) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	n.edges = append(n.edges, e)
	if idx != num {
		copy(n.edges[idx+1:], n.edges[idx:num])
		n.edges[idx] = e
	}
}

func (n *Node) replaceEdge(e edge) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	if idx < num && n.edges[idx].label == e.label {
		n.edges[idx].node = e.node
		return
	}
	panic("replacing missing edge")
}

func (n *Node) getEdge(label byte) (int, *Node) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		return idx, n.edges[idx].node
	}
	return -1, nil
}

func (n *Node) delEdge(label byte) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		copy(n.edges[idx:], n.edges[idx+1:])
		n.edges[len(n.edges)-1] = edge{}
		n.edges = n.edges[:len(n.edges)-1]
	}
}

func (n *Node) GetWatch(k []byte) (<-chan struct{}, interface{}, bool) {
	search := k
	watch := n.mutateCh
	for {

		if len(search) == 0 {
			if n.isLeaf() {
				return n.leaf.mutateCh, n.leaf.val, true
			}
			break
		}

		_, n = n.getEdge(search[0])
		if n == nil {
			break
		}

		watch = n.mutateCh

		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
	return watch, nil, false
}

func (n *Node) Get(k []byte) (interface{}, bool) {
	_, val, ok := n.GetWatch(k)
	return val, ok
}

func (n *Node) LongestPrefix(k []byte) ([]byte, interface{}, bool) {
	var last *leafNode
	search := k
	for {

		if n.isLeaf() {
			last = n.leaf
		}

		if len(search) == 0 {
			break
		}

		_, n = n.getEdge(search[0])
		if n == nil {
			break
		}

		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
	if last != nil {
		return last.key, last.val, true
	}
	return nil, nil, false
}

func (n *Node) Minimum() ([]byte, interface{}, bool) {
	for {
		if n.isLeaf() {
			return n.leaf.key, n.leaf.val, true
		}
		if len(n.edges) > 0 {
			n = n.edges[0].node
		} else {
			break
		}
	}
	return nil, nil, false
}

func (n *Node) Maximum() ([]byte, interface{}, bool) {
	for {
		if num := len(n.edges); num > 0 {
			n = n.edges[num-1].node
			continue
		}
		if n.isLeaf() {
			return n.leaf.key, n.leaf.val, true
		} else {
			break
		}
	}
	return nil, nil, false
}

func (n *Node) Iterator() *Iterator {
	return &Iterator{node: n}
}

func (n *Node) rawIterator() *rawIterator {
	iter := &rawIterator{node: n}
	iter.Next()
	return iter
}

func (n *Node) Walk(fn WalkFn) {
	recursiveWalk(n, fn)
}

func (n *Node) WalkPrefix(prefix []byte, fn WalkFn) {
	search := prefix
	for {

		if len(search) == 0 {
			recursiveWalk(n, fn)
			return
		}

		_, n = n.getEdge(search[0])
		if n == nil {
			break
		}

		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]

		} else if bytes.HasPrefix(n.prefix, search) {

			recursiveWalk(n, fn)
			return
		} else {
			break
		}
	}
}

func (n *Node) WalkPath(path []byte, fn WalkFn) {
	search := path
	for {

		if n.leaf != nil && fn(n.leaf.key, n.leaf.val) {
			return
		}

		if len(search) == 0 {
			return
		}

		_, n = n.getEdge(search[0])
		if n == nil {
			return
		}

		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
}

func recursiveWalk(n *Node, fn WalkFn) bool {

	if n.leaf != nil && fn(n.leaf.key, n.leaf.val) {
		return true
	}

	for _, e := range n.edges {
		if recursiveWalk(e.node, fn) {
			return true
		}
	}
	return false
}
