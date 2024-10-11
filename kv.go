package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"syscall"

	"golang.org/x/sys/unix"
	"golang.org/x/sys/windows"
)

type KV struct {
	Path  string
	Fsync func(int) error // overridable; for testing
	// internals
	fd   int
	tree BTree
	free FreeList
	mmap struct {
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64            // database size in number of pages
		nappend uint64            // no. of pages to be appended
		updates map[uint64][]byte //pending updates
	}
	failed bool // Did the last update fail?
}

// `BTree.get`, read a page.
func (db *KV) pageRead(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

func (db *KV) pageAlloc(node []byte) uint64 {
	assert(len(node) == BTREE_PAGE_SIZE)
	if ptr := db.free.PopHead(); ptr != 0 {
		assert(db.page.updates[ptr] == nil)
		db.page.updates[ptr] = node
		return ptr
	}

	return db.pageAppend(node)
}

func (db *KV) pageWrite(ptr uint64) []byte {
	assert(ptr < db.page.flushed+db.page.nappend)
	if node, ok := db.page.updates[ptr]; ok {
		return node
	}

	node := make([]byte, BTREE_PAGE_SIZE)
	copy(node, db.pageReadFile(ptr))
	db.page.updates[ptr] = node

	return node
}

func (db *KV) pageReadFile(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad pointer")
}

// `BTree.new`, allocate a new page.
func (db *KV) pageAppend(node []byte) uint64 {
	assert(len(node) == BTREE_PAGE_SIZE)
	ptr := db.page.flushed + db.page.nappend // just append
	db.page.nappend++
	assert(db.page.updates[ptr] == nil)
	db.page.updates[ptr] = node

	return ptr
}

// open or create a file and fsync the directory
func createFileSync(file string) (int, error) {
	// obtain the directory fd
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)
	// open or create the file
	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(dirfd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}
	// fsync the directory
	err = syscall.Fsync(dirfd)
	if err != nil { // may leave an empty file
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("fsync directory: %w", err)
	}
	// done
	return fd, nil
}

// open or create a DB file
func (db *KV) Open() error {
	if db.Fsync == nil {
		db.Fsync = syscall.Fsync
	}
	var err error
	// B+tree callbacks
	db.tree.get = db.pageRead
	db.tree.new = db.pageAlloc
	db.tree.del = db.free.PushTail

	// freelist callbacks
	db.free.get = db.pageRead
	db.free.new = db.pageAppend
	db.free.set = db.pageWrite

	// open or create the DB file
	if db.fd, err = createFileSync(db.Path); err != nil {
		return err
	}
	// get the file size
	finfo := syscall.Stat_t{}
	if err = syscall.Fstat(db.fd, &finfo); err != nil {
		goto fail
	}
	// create the initial mmap
	if err = extendMmap(db, int(finfo.Size)); err != nil {
		goto fail
	}
	// read the meta page
	if err = readRoot(db, finfo.Size); err != nil {
		goto fail
	}
	return nil
	// error
fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

const DB_SIG = "BuildYourOwnDB06"

// the 1st page stores the root pointer and other auxiliary data.
// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |
func loadMeta(db *KV, data []byte) {
	db.tree.root = binary.LittleEndian.Uint64(data[16:])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:])
}

func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize%BTREE_PAGE_SIZE != 0 {
		return errors.New("file is not a multiple of pages")
	}
	if fileSize == 0 { // empty file
		db.page.flushed = 2 // reserve 2 pages: meta & free list node

		db.free.headPage = 1
		db.free.tailPage = 1

		return nil
	}
	// read the page
	data := db.mmap.chunks[0]
	loadMeta(db, data)

	db.free.SetMaxSeq()

	// verify the page
	bad := !bytes.Equal([]byte(DB_SIG), data[:16])
	// pointers are within range?
	maxpages := uint64(fileSize / BTREE_PAGE_SIZE)
	bad = bad || !(0 < db.page.flushed && db.page.flushed <= maxpages)
	bad = bad || !(0 < db.tree.root && db.tree.root < db.page.flushed)
	bad = bad || !(0 < db.free.headPage && db.free.headPage < db.page.flushed)
	bad = bad || !(0 < db.free.tailPage && db.free.tailPage < db.page.flushed)
	if bad {
		return errors.New("bad meta page")
	}
	return nil
}

// update the meta page. it must be atomic.
func updateRoot(db *KV) error {
	// NOTE: atomic?
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

// extend the mmap by adding new mappings.
func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil // enough range
	}
	alloc := max(db.mmap.total, 64<<20) // double the current address space
	for db.mmap.total+alloc < size {
		alloc *= 2 // still not enough?
	}
	chunk, err := syscall.Mmap(
		db.fd, int64(db.mmap.total), alloc,
		syscall.PROT_READ, syscall.MAP_SHARED, // read-only
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func updateFile(db *KV) error {
	// 1. Write new nodes.
	if err := writePages(db); err != nil {
		return err
	}
	// 2. `fsync` to enforce the order between 1 and 3.
	if err := db.Fsync(db.fd); err != nil {
		return err
	}
	// 3. Update the root pointer atomically.
	if err := updateRoot(db); err != nil {
		return err
	}
	// 4. `fsync` to make everything persistent.
	if err := db.Fsync(db.fd); err != nil {
		return err
	}

	// preparing free list for next update
	db.free.SetMaxSeq()
	return nil
}

func updateOrRevert(db *KV, meta []byte) error {
	// ensure the on-disk meta page matches the in-memory one after an error
	if db.failed {
		if _, err := syscall.Pwrite(db.fd, meta, 0); err != nil {
			return fmt.Errorf("rewrite meta page: %w", err)
		}
		if err := db.Fsync(db.fd); err != nil {
			return err
		}
		db.failed = false
	}
	// 2-phase update
	err := updateFile(db)
	// revert on error
	if err != nil {
		// the on-disk meta page is in an unknown state.
		// mark it to be rewritten on later recovery.
		db.failed = true
		// in-memory states are reverted immediately to allow reads
		loadMeta(db, meta)
		// discard temporaries
		db.page.nappend = 0
		db.page.updates = map[uint64][]byte{}
	}
	return err
}

func writePages(db *KV) error {
	// extend the mmap if needed
	size := (int(db.page.flushed + db.page.nappend)) * BTREE_PAGE_SIZE
	if err := extendMmap(db, size); err != nil {
		return err
	}
	// write data pages to the file
	for ptr, node := range db.page.updates {
		offset := int64(ptr * BTREE_PAGE_SIZE)
		if _, err := unix.Pwrite(db.fd, node, offset); err != nil {
			return err
		}
	}
	// discard in-memory data
	db.page.flushed += db.page.nappend
	db.page.nappend = 0
	db.page.updates = map[uint64][]byte{}
	return nil
}

// KV interfaces
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}
func (db *KV) Set(key []byte, val []byte) (bool, error) {
	return db.Update(&UpdateReq{Key: key, Val: val})
}

func (db *KV) Update(req *UpdateReq) (bool, error) {
	meta := saveMeta(db)
	if updated, err := db.tree.Update(req); !updated {
		return false, err
	}
	err := updateOrRevert(db, meta)
	return err == nil, err
}

func (db *KV) Del(key []byte) (bool, error) {
	meta := saveMeta(db)
	if deleted, err := db.tree.Delete(key); !deleted {
		return false, err
	}
	err := updateOrRevert(db, meta)
	return err == nil, err
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil)
	}
	_ = syscall.Close(syscall.Handle(db.fd))
}
