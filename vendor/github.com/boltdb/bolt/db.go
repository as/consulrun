package bolt

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const maxMmapStep = 1 << 30 // 1GB

const version = 2

const magic uint32 = 0xED0CDAED

const IgnoreNoSync = runtime.GOOS == "openbsd"

const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
	DefaultAllocSize         = 16 * 1024 * 1024
)

var defaultPageSize = os.Getpagesize()

type DB struct {
	StrictMode bool

	//

	//

	NoSync bool

	//

	NoGrowSync bool

	MmapFlags int

	//

	//

	MaxBatchSize int

	//

	//

	MaxBatchDelay time.Duration

	AllocSize int

	path     string
	file     *os.File
	lockfile *os.File // windows only
	dataref  []byte   // mmap'ed readonly, write throws SEGV
	data     *[maxMapSize]byte
	datasz   int
	filesz   int // current on disk file size
	meta0    *meta
	meta1    *meta
	pageSize int
	opened   bool
	rwtx     *Tx
	txs      []*Tx
	freelist *freelist
	stats    Stats

	pagePool sync.Pool

	batchMu sync.Mutex
	batch   *batch

	rwlock   sync.Mutex   // Allows only one writer at a time.
	metalock sync.Mutex   // Protects meta page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
	statlock sync.RWMutex // Protects stats access.

	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	readOnly bool
}

func (db *DB) Path() string {
	return db.path
}

func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path:%q}", db.path)
}

func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var db = &DB{opened: true}

	if options == nil {
		options = DefaultOptions
	}
	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags

	db.MaxBatchSize = DefaultMaxBatchSize
	db.MaxBatchDelay = DefaultMaxBatchDelay
	db.AllocSize = DefaultAllocSize

	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return nil, err
	}

	if err := flock(db, mode, !db.readOnly, options.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	db.ops.writeAt = db.file.WriteAt

	if info, err := db.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {

		if err := db.init(); err != nil {
			return nil, err
		}
	} else {

		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {

				//

				db.pageSize = os.Getpagesize()
			} else {
				db.pageSize = int(m.pageSize)
			}
		}
	}

	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	if err := db.mmap(options.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	db.freelist = newFreelist()
	db.freelist.read(db.page(db.meta().freelist))

	return db, nil
}

func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	if err := db.munmap(); err != nil {
		return err
	}

	if err := mmap(db, size); err != nil {
		return err
	}

	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

func (db *DB) mmapSize(size int) (int, error) {

	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

func (db *DB) init() error {

	db.pageSize = os.Getpagesize()

	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag

		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2
		m.root = bucket{root: 3}
		m.pgid = 4
		m.txid = txid(i)
		m.checksum = m.sum64()
	}

	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(db); err != nil {
		return err
	}

	return nil
}

func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false

	db.freelist = nil

	db.ops.writeAt = nil

	if err := db.munmap(); err != nil {
		return err
	}

	if db.file != nil {

		if !db.readOnly {

			if err := funlock(db); err != nil {
				log.Printf("bolt.Close(): funlock error: %s", err)
			}
		}

		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close: %s", err)
		}
		db.file = nil
	}

	db.path = ""
	return nil
}

//
//
//
func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

func (db *DB) beginTx() (*Tx, error) {

	db.metalock.Lock()

	db.mmaplock.RLock()

	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	t := &Tx{}
	t.init(db)

	db.txs = append(db.txs, t)
	n := len(db.txs)

	db.metalock.Unlock()

	db.statlock.Lock()
	db.stats.TxN++
	db.stats.OpenTxN = n
	db.statlock.Unlock()

	return t, nil
}

func (db *DB) beginRWTx() (*Tx, error) {

	if db.readOnly {
		return nil, ErrDatabaseReadOnly
	}

	db.rwlock.Lock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t

	var minid txid = 0xFFFFFFFFFFFFFFFF
	for _, t := range db.txs {
		if t.meta.txid < minid {
			minid = t.meta.txid
		}
	}
	if minid > 0 {
		db.freelist.release(minid - 1)
	}

	return t, nil
}

func (db *DB) removeTx(tx *Tx) {

	db.mmaplock.RUnlock()

	db.metalock.Lock()

	for i, t := range db.txs {
		if t == tx {
			last := len(db.txs) - 1
			db.txs[i] = db.txs[last]
			db.txs[last] = nil
			db.txs = db.txs[:last]
			break
		}
	}
	n := len(db.txs)

	db.metalock.Unlock()

	db.statlock.Lock()
	db.stats.OpenTxN = n
	db.stats.TxStats.add(&tx.stats)
	db.statlock.Unlock()
}

//
func (db *DB) Update(fn func(*Tx) error) error {
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	t.managed = true

	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

//
func (db *DB) View(fn func(*Tx) error) error {
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	t.managed = true

	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	if err := t.Rollback(); err != nil {
		return err
	}

	return nil
}

//
//
//
//
//
func (db *DB) Batch(fn func(*Tx) error) error {
	errCh := make(chan error, 1)

	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {

		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {

		go db.batch.trigger()
	}
	db.batchMu.Unlock()

	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

type call struct {
	fn  func(*Tx) error
	err chan<- error
}

type batch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []call
}

func (b *batch) trigger() {
	b.start.Do(b.run)
}

func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()

	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {

			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]

			c.err <- trySolo
			continue retry
		}

		for _, c := range b.calls {
			if c.err != nil {
				c.err <- err
			}
		}
		break retry
	}
}

var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

//
func (db *DB) Sync() error { return fdatasync(db) }

func (db *DB) Stats() Stats {
	db.statlock.RLock()
	defer db.statlock.RUnlock()
	return db.stats
}

func (db *DB) Info() *Info {
	return &Info{uintptr(unsafe.Pointer(&db.data[0])), db.pageSize}
}

func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

func (db *DB) meta() *meta {

	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.txid > db.meta0.txid {
		metaA = db.meta1
		metaB = db.meta0
	}

	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	panic("bolt.DB.meta(): invalid meta pages")
}

func (db *DB) allocate(count int) (*page, error) {

	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}

	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

func (db *DB) grow(sz int) error {

	if sz <= db.filesz {
		return nil
	}

	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}

	db.filesz = sz
	return nil
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

type Options struct {
	Timeout time.Duration

	NoGrowSync bool

	ReadOnly bool

	MmapFlags int

	//

	InitialMmapSize int
}

var DefaultOptions = &Options{
	Timeout:    0,
	NoGrowSync: false,
}

type Stats struct {
	FreePageN     int // total number of free pages on the freelist
	PendingPageN  int // total number of pending pages on the freelist
	FreeAlloc     int // total bytes allocated in free pages
	FreelistInuse int // total bytes used by the freelist

	TxN     int // total number of started read transactions
	OpenTxN int // number of currently open read transactions

	TxStats TxStats // global, ongoing stats.
}

func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}
	var diff Stats
	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

func (s *Stats) add(other *Stats) {
	s.TxStats.add(&other.TxStats)
}

type Info struct {
	Data     uintptr
	PageSize int
}

type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	root     bucket
	freelist pgid
	pgid     pgid
	txid     txid
	checksum uint64
}

func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

func (m *meta) copy(dest *meta) {
	*dest = *m
}

func (m *meta) write(p *page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	m.checksum = m.sum64()

	m.copy(p.meta())
}

func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

func printstack() {
	stack := strings.Join(strings.Split(string(debug.Stack()), "\n")[2:], "\n")
	fmt.Fprintln(os.Stderr, stack)
}
