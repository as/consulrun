package bolt

import (
	"fmt"
	"sort"
	"unsafe"
)

type freelist struct {
	ids     []pgid          // all free and available free page ids.
	pending map[txid][]pgid // mapping of soon-to-be free page ids by tx.
	cache   map[pgid]bool   // fast lookup of all free and pending page ids.
}

func newFreelist() *freelist {
	return &freelist{
		pending: make(map[txid][]pgid),
		cache:   make(map[pgid]bool),
	}
}

func (f *freelist) size() int {
	n := f.count()
	if n >= 0xFFFF {

		n++
	}
	return pageHeaderSize + (int(unsafe.Sizeof(pgid(0))) * n)
}

func (f *freelist) count() int {
	return f.free_count() + f.pending_count()
}

func (f *freelist) free_count() int {
	return len(f.ids)
}

func (f *freelist) pending_count() int {
	var count int
	for _, list := range f.pending {
		count += len(list)
	}
	return count
}

func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	for _, list := range f.pending {
		m = append(m, list...)
	}
	sort.Sort(m)
	mergepgids(dst, f.ids, m)
}

func (f *freelist) allocate(n int) pgid {
	if len(f.ids) == 0 {
		return 0
	}

	var initial, previd pgid
	for i, id := range f.ids {
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		if previd == 0 || id-previd != 1 {
			initial = id
		}

		if (id-initial)+1 == pgid(n) {

			if (i + 1) == n {
				f.ids = f.ids[i+1:]
			} else {
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}

			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}

			return initial
		}

		previd = id
	}
	return 0
}

func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	var ids = f.pending[txid]
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {

		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}

		ids = append(ids, id)
		f.cache[id] = true
	}
	f.pending[txid] = ids
}

func (f *freelist) release(txid txid) {
	m := make(pgids, 0)
	for tid, ids := range f.pending {
		if tid <= txid {

			m = append(m, ids...)
			delete(f.pending, tid)
		}
	}
	sort.Sort(m)
	f.ids = pgids(f.ids).merge(m)
}

func (f *freelist) rollback(txid txid) {

	for _, id := range f.pending[txid] {
		delete(f.cache, id)
	}

	delete(f.pending, txid)
}

func (f *freelist) freed(pgid pgid) bool {
	return f.cache[pgid]
}

func (f *freelist) read(p *page) {

	idx, count := 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		count = int(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0])
	}

	if count == 0 {
		f.ids = nil
	} else {
		ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.ids = make([]pgid, len(ids))
		copy(f.ids, ids)

		sort.Sort(pgids(f.ids))
	}

	f.reindex()
}

func (f *freelist) write(p *page) error {

	p.flags |= freelistPageFlag

	lenids := f.count()
	if lenids == 0 {
		p.count = uint16(lenids)
	} else if lenids < 0xFFFF {
		p.count = uint16(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[:])
	} else {
		p.count = 0xFFFF
		((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0] = pgid(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[1:])
	}

	return nil
}

func (f *freelist) reload(p *page) {
	f.read(p)

	pcache := make(map[pgid]bool)
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			pcache[pendingID] = true
		}
	}

	var a []pgid
	for _, id := range f.ids {
		if !pcache[id] {
			a = append(a, id)
		}
	}
	f.ids = a

	f.reindex()
}

// reindex rebuilds the free cache based on available and pending free lists.
func (f *freelist) reindex() {
	f.cache = make(map[pgid]bool, len(f.ids))
	for _, id := range f.ids {
		f.cache[id] = true
	}
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			f.cache[pendingID] = true
		}
	}
}
