package main

import (
	"bytes"
	"errors"
	"runtime"
	"slices"
)

type KVTX struct {
	snapshot BTree // read-only snapshot(copy on wrrite)
	version  uint64
	// local updates are held in an memory B+tree
	pending BTree      //captured KV updates
	reads   []KeyRange //list of involved interval of keys for dtecting conflicts
	// cheks for conflict even if update changes nothing
	updateAttempted bool
	done            bool
}

// start <=key <=stop
type KeyRange struct {
	start []byte
	stop  []byte
}

type TXSave struct {
	root  uint64
	reads []KeyRange
}

func (tx *KVTX) Save(save *TXSave) {
	save.root, save.reads = tx.pending.root, tx.reads
}

func (tx *KVTX) Revert(save *TXSave) {
	tx.pending.root, tx.reads = save.root, save.reads
}

const (
	FLAG_DELETED = byte(1)
	FLAG_UPDATED = byte(2)
)

// begin a transaction
func (kv *KV) Begin(tx *KVTX) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	tx.snapshot.root = kv.tree.root
	chunks := kv.mmap.chunks
	tx.snapshot.get = func(ptr uint64) []byte { return mmapRead(ptr, chunks) }
	tx.version = kv.version

	// in memeory tree to caputre updaets
	pages := [][]byte(nil)
	tx.pending.get = func(ptr uint64) []byte { return pages[ptr-1] }
	tx.pending.new = func(b []byte) uint64 {
		pages = append(pages, b)
		return uint64(len(pages))
	}
	tx.pending.del = func(uint64) {}
	// keepin track of concurrent TXs
	kv.ongoing = append(kv.ongoing, tx.version)
	runtime.SetFinalizer(tx, func(tx *KVTX) { assert(tx.done) })
}

// rollback on error
func (kv *KV) Commit(tx *KVTX) error {
	assert(!tx.done)
	tx.done = true
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	// check conflicts
	if tx.updateAttempted && detectConflicts(kv, tx) {
		return ErrorConflict
	}

	// save meta page
	meta, root := saveMeta(kv), kv.tree.root
	kv.free.curVer = kv.version + 1 //transfer current updates to current tree
	writes := []KeyRange(nil)
	for iter := tx.pending.Seek(nil, CMP_GT); iter.Valid(); iter.Next() {
		modified := false
		key, val := iter.Deref()
		oldVal, isOld := tx.snapshot.Get(key)
		switch val[0] {
		case FLAG_DELETED:
			modified = isOld
			deleted, err := kv.tree.Delete(&DeleteReq{Key: key})
			assert(err == nil)          // can only fail by length limit
			assert(deleted == modified) // assured by conflict detection

		case FLAG_UPDATED:
			modified = (!isOld || !bytes.Equal(oldVal, val[1:]))
			updated, err := kv.tree.Update(&UpdateReq{Key: key, Val: val[1:]})
			assert(err == nil)
			assert(updated == modified)

		default:
			panic("unreachable")
		}

		if modified && len(kv.ongoing) > 1 {
			writes = append(writes, KeyRange{key, key})
		}
	}

	// commitin update
	if root != kv.tree.root {
		kv.version++
		if err := updateOrRevert(kv, meta); err != nil {
			return err
		}
	}

	if len(writes) > 0 {
		slices.SortFunc(writes, func(r1, r2 KeyRange) int {
			return bytes.Compare(r1.start, r2.start)
		})
		kv.history = append(kv.history, CommittedTX{kv.version, writes})
	}
	return nil
}

// end transaction
func (kv *KV) Abort(tx *KVTX) {
	assert(!tx.done)
	tx.done = true

	kv.mutex.Lock()
	txFinalize(kv, tx)
	kv.mutex.Unlock()
}

var ErrorConflict = errors.New("cannot commit due to conflict")

func detectConflicts(kv *KV, tx *KVTX) bool {
	slices.SortFunc(tx.reads, func(r1, r2 KeyRange) int {
		return bytes.Compare(r1.start, r2.start)
	})

	for i := len(kv.history) - 1; i >= 0; i-- {
		if !versionBefore(tx.version, kv.history[i].version) {
			break
		}
		if sortedRangesOverlap(tx.reads, kv.history[i].writes) {
			return true
		}
	}

	return false
}

func sortedRangesOverlap(s1, s2 []KeyRange) bool {
	for len(s1) > 0 && len(s2) > 0 {
		if bytes.Compare(s1[0].stop, s2[0].start) < 0 {
			s1 = s1[1:]
		} else if bytes.Compare(s2[0].stop, s1[0].start) < 0 {
			s2 = s2[1:]
		} else {
			return true
		}
	}

	return false
}

// routines when exiting a transacion
func txFinalize(kv *KV, tx *KVTX) {
	idx := slices.Index(kv.ongoing, tx.version)
	last := len(kv.ongoing) - 1
	kv.ongoing[idx], kv.ongoing = kv.ongoing[last], kv.ongoing[:last]

	// oldest in use version
	minVer := kv.version
	for _, other := range kv.ongoing {
		if versionBefore(other, minVer) {
			minVer = other
		}
	}

	// release free list
	kv.free.SetMaxVer(minVer)

	for idx = 0; idx < len(kv.history); idx++ {
		if versionBefore(minVer, kv.history[idx].version) {
			break
		}
	}

	kv.history = kv.history[idx:]
}

// KV interfaces
type KVIter interface {
	Deref() (key []byte, val []byte)
	Valid() bool
	Next()
}

// combines pending updates and the snapshot
type CombinedIterator struct {
	top *BIter //kvtx pending
	bot *BIter //kvtx snapshot
	dir int    //+1 for greater or greater than, -1 for less or less than

	//end of range
	cmp int
	end []byte
}

func (iter *CombinedIterator) Deref() ([]byte, []byte) {
	var k1, k2, v1, v2 []byte
	top, bot := iter.top.Valid(), iter.bot.Valid()
	assert(top || bot)
	if top {
		k1, v1 = iter.top.Deref()
	}
	if bot {
		k2, v2 = iter.bot.Deref()
	}

	// usin min/max key of the two
	if top && bot && bytes.Compare(k1, k2) == +iter.dir {
		return k2, v2
	}
	if top {
		return k1, v1[1:]
	} else {
		return k2, v2
	}
}

func (iter *CombinedIterator) Valid() bool {
	if iter.top.Valid() || iter.bot.Valid() {
		key, _ := iter.Deref()
		return cmpOk(key, iter.cmp, iter.end)
	}

	return false
}

func (iter *CombinedIterator) Next() {
	top, bot := iter.top.Valid(), iter.bot.Valid()
	if top && bot {
		k1, _ := iter.top.Deref()
		k2, _ := iter.bot.Deref()

		switch bytes.Compare(k1, k2) {
		case -iter.dir:
			top, bot = true, false
		case +iter.dir:
			top, bot = false, true
		case 0: // equal; move both
		}
	}

	assert(top || bot)
	if top {
		if iter.dir > 0 {
			iter.top.Next()
		} else {
			iter.top.Prev()
		}
	}

	if bot {
		if iter.dir > 0 {
			iter.bot.Next()
		} else {
			iter.bot.Prev()
		}
	}

}

func cmp2Dir(cmp int) int {
	if cmp > 0 {
		return +1
	} else {
		return -1
	}
}

// range query combines captured updates with snapshots
func (tx *KVTX) Seek(key1 []byte, cmp1 int, key2 []byte, cmp2 int) KVIter {
	assert(cmp2Dir(cmp1) != cmp2Dir(cmp2))
	lo, hi := key1, key2
	if cmp2Dir(cmp1) < 0 {
		lo, hi = hi, lo
	}
	tx.reads = append(tx.reads, KeyRange{lo, hi})

	return &CombinedIterator{
		top: tx.pending.Seek(key1, cmp1),
		bot: tx.pending.Seek(key1, cmp1),
		dir: cmp2Dir(cmp1),
		cmp: cmp2,
		end: key2,
	}
}

func (tx *KVTX) Update(req *UpdateReq) (bool, error) {
	tx.updateAttempted = true

	old, exists := tx.Get(req.Key)
	if req.Mode == MODE_UPDATE_ONLY && !exists {
		return false, nil
	}
	if req.Mode == MODE_INSERT_ONLY && exists {
		return false, nil
	}
	if exists && bytes.Equal(old, req.Val) {
		return false, nil
	}

	flaggedVal := append([]byte{FLAG_UPDATED}, req.Val...)
	_, err := tx.pending.Update(&UpdateReq{Key: req.Key, Val: flaggedVal})
	if err != nil {
		return false, err
	}

	req.Added = !exists
	req.Updated = true
	req.Old = old

	return true, nil
}

func (tx *KVTX) Del(req *DeleteReq) (bool, error) {
	tx.updateAttempted = true
	exists := false
	if req.Old, exists = tx.Get(req.Key); !exists {
		return false, nil
	}

	return tx.pending.Update(&UpdateReq{Key: req.Key, Val: []byte{FLAG_DELETED}})
}

func (tx *KVTX) Set(key []byte, val []byte) (bool, error) {
	return tx.Update(&UpdateReq{Key: key, Val: val})
}

// point query combines captured updates with snapshots
func (tx *KVTX) Get(key []byte) ([]byte, bool) {
	tx.reads = append(tx.reads, KeyRange{key, key})
	val, ok := tx.pending.Get(key)

	switch {
	case ok && val[0] == FLAG_UPDATED: //updated in this tx
		return val[1:], true
	case ok && val[0] == FLAG_DELETED: //deleted in this TX
		return nil, false
	case !ok:
		return tx.snapshot.Get(key)

	default:
		panic("unreachable")
	}
}
