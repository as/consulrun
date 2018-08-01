package consul

import (
	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
)

type dirEntFilter struct {
	acl acl.ACL
	ent structs.DirEntries
}

func (d *dirEntFilter) Len() int {
	return len(d.ent)
}
func (d *dirEntFilter) Filter(i int) bool {
	return !d.acl.KeyRead(d.ent[i].Key)
}
func (d *dirEntFilter) Move(dst, src, span int) {
	copy(d.ent[dst:dst+span], d.ent[src:src+span])
}

func FilterDirEnt(acl acl.ACL, ent structs.DirEntries) structs.DirEntries {
	df := dirEntFilter{acl: acl, ent: ent}
	return ent[:FilterEntries(&df)]
}

type keyFilter struct {
	acl  acl.ACL
	keys []string
}

func (k *keyFilter) Len() int {
	return len(k.keys)
}
func (k *keyFilter) Filter(i int) bool {
	return !k.acl.KeyRead(k.keys[i])
}

func (k *keyFilter) Move(dst, src, span int) {
	copy(k.keys[dst:dst+span], k.keys[src:src+span])
}

func FilterKeys(acl acl.ACL, keys []string) []string {
	kf := keyFilter{acl: acl, keys: keys}
	return keys[:FilterEntries(&kf)]
}

type txnResultsFilter struct {
	acl     acl.ACL
	results structs.TxnResults
}

func (t *txnResultsFilter) Len() int {
	return len(t.results)
}

func (t *txnResultsFilter) Filter(i int) bool {
	result := t.results[i]
	if result.KV != nil {
		return !t.acl.KeyRead(result.KV.Key)
	}
	return false
}

func (t *txnResultsFilter) Move(dst, src, span int) {
	copy(t.results[dst:dst+span], t.results[src:src+span])
}

func FilterTxnResults(acl acl.ACL, results structs.TxnResults) structs.TxnResults {
	rf := txnResultsFilter{acl: acl, results: results}
	return results[:FilterEntries(&rf)]
}

type Filter interface {
	Len() int
	Filter(int) bool
	Move(dst, src, span int)
}

func FilterEntries(f Filter) int {

	dst := 0
	src := 0
	n := f.Len()
	for dst < n {
		for src < n && f.Filter(src) {
			src++
		}
		if src == n {
			break
		}
		end := src + 1
		for end < n && !f.Filter(end) {
			end++
		}
		span := end - src
		if span > 0 {
			f.Move(dst, src, span)
			dst += span
			src += span
		}
	}

	return dst
}
