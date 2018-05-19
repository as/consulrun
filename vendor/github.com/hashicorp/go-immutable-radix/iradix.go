package iradix

import (
	"bytes"
	"strings"

	"github.com/hashicorp/golang-lru/simplelru"
)

const (
	defaultModifiedCache = 8192
)

type Tree struct {
	root *Node
	size int
}

func New() *Tree {
	t := &Tree{
		root: &Node{
			mutateCh: make(chan struct{}),
		},
	}
	return t
}

func (t *Tree) Len() int {
	return t.size
}

type Txn struct {
	root *Node

	snap *Node

	size int

	writable *simplelru.LRU

	trackChannels map[chan struct{}]struct{}
	trackOverflow bool
	trackMutate   bool
}

func (t *Tree) Txn() *Txn {
	txn := &Txn{
		root: t.root,
		snap: t.root,
		size: t.size,
	}
	return txn
}

func (t *Txn) TrackMutate(track bool) {
	t.trackMutate = track
}

func (t *Txn) trackChannel(ch chan struct{}) {

	if t.trackOverflow {
		return
	}

	if len(t.trackChannels) >= defaultModifiedCache {

		t.trackOverflow = true

		t.trackChannels = nil
		return
	}

	if t.trackChannels == nil {
		t.trackChannels = make(map[chan struct{}]struct{})
	}

	t.trackChannels[ch] = struct{}{}
}

func (t *Txn) writeNode(n *Node, forLeafUpdate bool) *Node {

	if t.writable == nil {
		lru, err := simplelru.NewLRU(defaultModifiedCache, nil)
		if err != nil {
			panic(err)
		}
		t.writable = lru
	}

	if _, ok := t.writable.Get(n); ok {
		if t.trackMutate && forLeafUpdate && n.leaf != nil {
			t.trackChannel(n.leaf.mutateCh)
		}
		return n
	}

	if t.trackMutate {
		t.trackChannel(n.mutateCh)
	}

	if t.trackMutate && forLeafUpdate && n.leaf != nil {
		t.trackChannel(n.leaf.mutateCh)
	}

	nc := &Node{
		mutateCh: make(chan struct{}),
		leaf:     n.leaf,
	}
	if n.prefix != nil {
		nc.prefix = make([]byte, len(n.prefix))
		copy(nc.prefix, n.prefix)
	}
	if len(n.edges) != 0 {
		nc.edges = make([]edge, len(n.edges))
		copy(nc.edges, n.edges)
	}

	t.writable.Add(nc, nil)
	return nc
}

func (t *Txn) trackChannelsAndCount(n *Node) int {

	leaves := 0
	if n.leaf != nil {
		leaves = 1
	}

	if t.trackMutate {
		t.trackChannel(n.mutateCh)
	}

	if t.trackMutate && n.leaf != nil {
		t.trackChannel(n.leaf.mutateCh)
	}

	for _, e := range n.edges {
		leaves += t.trackChannelsAndCount(e.node)
	}
	return leaves
}

func (t *Txn) mergeChild(n *Node) {

	e := n.edges[0]
	child := e.node
	if t.trackMutate {
		t.trackChannel(child.mutateCh)
	}

	n.prefix = concat(n.prefix, child.prefix)
	n.leaf = child.leaf
	if len(child.edges) != 0 {
		n.edges = make([]edge, len(child.edges))
		copy(n.edges, child.edges)
	} else {
		n.edges = nil
	}
}

func (t *Txn) insert(n *Node, k, search []byte, v interface{}) (*Node, interface{}, bool) {

	if len(search) == 0 {
		var oldVal interface{}
		didUpdate := false
		if n.isLeaf() {
			oldVal = n.leaf.val
			didUpdate = true
		}

		nc := t.writeNode(n, true)
		nc.leaf = &leafNode{
			mutateCh: make(chan struct{}),
			key:      k,
			val:      v,
		}
		return nc, oldVal, didUpdate
	}

	idx, child := n.getEdge(search[0])

	if child == nil {
		e := edge{
			label: search[0],
			node: &Node{
				mutateCh: make(chan struct{}),
				leaf: &leafNode{
					mutateCh: make(chan struct{}),
					key:      k,
					val:      v,
				},
				prefix: search,
			},
		}
		nc := t.writeNode(n, false)
		nc.addEdge(e)
		return nc, nil, false
	}

	commonPrefix := longestPrefix(search, child.prefix)
	if commonPrefix == len(child.prefix) {
		search = search[commonPrefix:]
		newChild, oldVal, didUpdate := t.insert(child, k, search, v)
		if newChild != nil {
			nc := t.writeNode(n, false)
			nc.edges[idx].node = newChild
			return nc, oldVal, didUpdate
		}
		return nil, oldVal, didUpdate
	}

	nc := t.writeNode(n, false)
	splitNode := &Node{
		mutateCh: make(chan struct{}),
		prefix:   search[:commonPrefix],
	}
	nc.replaceEdge(edge{
		label: search[0],
		node:  splitNode,
	})

	modChild := t.writeNode(child, false)
	splitNode.addEdge(edge{
		label: modChild.prefix[commonPrefix],
		node:  modChild,
	})
	modChild.prefix = modChild.prefix[commonPrefix:]

	leaf := &leafNode{
		mutateCh: make(chan struct{}),
		key:      k,
		val:      v,
	}

	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.leaf = leaf
		return nc, nil, false
	}

	splitNode.addEdge(edge{
		label: search[0],
		node: &Node{
			mutateCh: make(chan struct{}),
			leaf:     leaf,
			prefix:   search,
		},
	})
	return nc, nil, false
}

func (t *Txn) delete(parent, n *Node, search []byte) (*Node, *leafNode) {

	if len(search) == 0 {
		if !n.isLeaf() {
			return nil, nil
		}

		nc := t.writeNode(n, true)
		nc.leaf = nil

		if n != t.root && len(nc.edges) == 1 {
			t.mergeChild(nc)
		}
		return nc, n.leaf
	}

	label := search[0]
	idx, child := n.getEdge(label)
	if child == nil || !bytes.HasPrefix(search, child.prefix) {
		return nil, nil
	}

	search = search[len(child.prefix):]
	newChild, leaf := t.delete(n, child, search)
	if newChild == nil {
		return nil, nil
	}

	nc := t.writeNode(n, false)

	if newChild.leaf == nil && len(newChild.edges) == 0 {
		nc.delEdge(label)
		if n != t.root && len(nc.edges) == 1 && !nc.isLeaf() {
			t.mergeChild(nc)
		}
	} else {
		nc.edges[idx].node = newChild
	}
	return nc, leaf
}

func (t *Txn) deletePrefix(parent, n *Node, search []byte) (*Node, int) {

	if len(search) == 0 {
		nc := t.writeNode(n, true)
		if n.isLeaf() {
			nc.leaf = nil
		}
		nc.edges = nil
		return nc, t.trackChannelsAndCount(n)
	}

	label := search[0]
	idx, child := n.getEdge(label)

	if child == nil || (!bytes.HasPrefix(child.prefix, search) && !bytes.HasPrefix(search, child.prefix)) {
		return nil, 0
	}

	if len(child.prefix) > len(search) {
		search = []byte("")
	} else {
		search = search[len(child.prefix):]
	}
	newChild, numDeletions := t.deletePrefix(n, child, search)
	if newChild == nil {
		return nil, 0
	}

	nc := t.writeNode(n, false)

	if newChild.leaf == nil && len(newChild.edges) == 0 {
		nc.delEdge(label)
		if n != t.root && len(nc.edges) == 1 && !nc.isLeaf() {
			t.mergeChild(nc)
		}
	} else {
		nc.edges[idx].node = newChild
	}
	return nc, numDeletions
}

func (t *Txn) Insert(k []byte, v interface{}) (interface{}, bool) {
	newRoot, oldVal, didUpdate := t.insert(t.root, k, k, v)
	if newRoot != nil {
		t.root = newRoot
	}
	if !didUpdate {
		t.size++
	}
	return oldVal, didUpdate
}

func (t *Txn) Delete(k []byte) (interface{}, bool) {
	newRoot, leaf := t.delete(nil, t.root, k)
	if newRoot != nil {
		t.root = newRoot
	}
	if leaf != nil {
		t.size--
		return leaf.val, true
	}
	return nil, false
}

func (t *Txn) DeletePrefix(prefix []byte) bool {
	newRoot, numDeletions := t.deletePrefix(nil, t.root, prefix)
	if newRoot != nil {
		t.root = newRoot
		t.size = t.size - numDeletions
		return true
	}
	return false

}

func (t *Txn) Root() *Node {
	return t.root
}

func (t *Txn) Get(k []byte) (interface{}, bool) {
	return t.root.Get(k)
}

func (t *Txn) GetWatch(k []byte) (<-chan struct{}, interface{}, bool) {
	return t.root.GetWatch(k)
}

func (t *Txn) Commit() *Tree {
	nt := t.CommitOnly()
	if t.trackMutate {
		t.Notify()
	}
	return nt
}

func (t *Txn) CommitOnly() *Tree {
	nt := &Tree{t.root, t.size}
	t.writable = nil
	return nt
}

func (t *Txn) slowNotify() {
	snapIter := t.snap.rawIterator()
	rootIter := t.root.rawIterator()
	for snapIter.Front() != nil || rootIter.Front() != nil {

		if snapIter.Front() == nil {
			return
		}
		snapElem := snapIter.Front()

		if rootIter.Front() == nil {
			close(snapElem.mutateCh)
			if snapElem.isLeaf() {
				close(snapElem.leaf.mutateCh)
			}
			snapIter.Next()
			continue
		}

		cmp := strings.Compare(snapIter.Path(), rootIter.Path())

		if cmp < 0 {
			close(snapElem.mutateCh)
			if snapElem.isLeaf() {
				close(snapElem.leaf.mutateCh)
			}
			snapIter.Next()
			continue
		}

		if cmp > 0 {
			rootIter.Next()
			continue
		}

		rootElem := rootIter.Front()
		if snapElem != rootElem {
			close(snapElem.mutateCh)
			if snapElem.leaf != nil && (snapElem.leaf != rootElem.leaf) {
				close(snapElem.leaf.mutateCh)
			}
		}
		snapIter.Next()
		rootIter.Next()
	}
}

func (t *Txn) Notify() {
	if !t.trackMutate {
		return
	}

	if t.trackOverflow {
		t.slowNotify()
	} else {
		for ch := range t.trackChannels {
			close(ch)
		}
	}

	t.trackChannels = nil
	t.trackOverflow = false
}

func (t *Tree) Insert(k []byte, v interface{}) (*Tree, interface{}, bool) {
	txn := t.Txn()
	old, ok := txn.Insert(k, v)
	return txn.Commit(), old, ok
}

func (t *Tree) Delete(k []byte) (*Tree, interface{}, bool) {
	txn := t.Txn()
	old, ok := txn.Delete(k)
	return txn.Commit(), old, ok
}

func (t *Tree) DeletePrefix(k []byte) (*Tree, bool) {
	txn := t.Txn()
	ok := txn.DeletePrefix(k)
	return txn.Commit(), ok
}

func (t *Tree) Root() *Node {
	return t.root
}

func (t *Tree) Get(k []byte) (interface{}, bool) {
	return t.root.Get(k)
}

func longestPrefix(k1, k2 []byte) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}
	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

func concat(a, b []byte) []byte {
	c := make([]byte, len(a)+len(b))
	copy(c, a)
	copy(c[len(a):], b)
	return c
}
