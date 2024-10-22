package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
)

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
	TYPE_INF   = 0xff
)

type DB struct {
	Path   string
	kv     KV
	mu     sync.Mutex
	tables map[string]*TableDef
}

type DBTX struct {
	kv KVTX
	db *DB
}

func (db *DB) Begin(tx *DBTX) {
	tx.db = db
	db.kv.Begin(&tx.kv)
}

func (db *DB) Commit(tx *DBTX) error {
	return db.kv.Commit(&tx.kv)
}

func (db *DB) Abort(tx *DBTX) {
	db.kv.Abort(&tx.kv)
}

func (tx *DBTX) Save(save *TXSave) {
	tx.kv.Save(save)
}

func (tx*DBTX) Revert(save *TXSave) {
	tx.kv.Revert(save)
}

type TableDef struct {
	Name     string
	Types    []uint32 //col type
	Cols     []string //col name
	Prefixes []uint32
	Indexes  [][]string
}

// table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// represents a list of col names and values
type Record struct {
	Cols []string
	Vals []Value
}

func (rec *Record) AddStr(col string, val []byte) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_BYTES, Str: val})

	return rec
}

func (rec *Record) AddInt64(col string, val int64) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_INT64, I64: val})

	return rec
}
func (rec *Record) Get(key string) *Value {
	for i, c := range rec.Cols {
		if c == key {
			return &rec.Vals[i]
		}
	}

	return nil
}

// INTERNAL TABLES
// store metadata
var TDEF_META = &TableDef{
	Name:     "@meta",
	Types:    []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:     []string{"key", "val"},
	Prefixes: []uint32{1},
	Indexes:  [][]string{{"key"}},
}

// store table schemas
var TDEF_TABLE = &TableDef{
	Name:     "@table",
	Types:    []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:     []string{"name", "def"},
	Prefixes: []uint32{2},
	Indexes:  [][]string{{"name"}},
}

var INTERNAL_TABLES map[string]*TableDef = map[string]*TableDef{
	"@meta":  TDEF_META,
	"@table": TDEF_TABLE,
}

// reorder records to defined col. order
func reorderRecord(tdef *TableDef, rec Record) ([]Value, error) {
	assert(len(rec.Cols) == len(rec.Vals))
	out := make([]Value, len(tdef.Cols))
	for i, c := range tdef.Cols {
		v := rec.Get(c)
		if v == nil {
			continue
		}
		if v.Type != tdef.Types[i] {
			return nil, fmt.Errorf("bad column type: %s", c)
		}
		out[i] = *v
	}

	return out, nil
}

func valuesComplete(tdef *TableDef, vals []Value, n int) error {
	for i, v := range vals {
		if i < n && v.Type == 0 {
			return fmt.Errorf("missing column: %s", tdef.Cols[i])
		} else if i >= n && v.Type != 0 {
			return fmt.Errorf("extra column: %s", tdef.Cols[i])
		}
	}

	return nil
}

// escape null byte so string doesnt contain no null byte
func escapeString(in []byte) []byte {
	toEscape := bytes.Count(in, []byte{0}) + bytes.Count(in, []byte{1})
	if toEscape == 0 {
		return in
	}

	out := make([]byte, len(in)+toEscape)
	pos := 0
	for _, ch := range in {
		if ch <= 1 {
			out[pos+0] = 0x01
			out[pos+1] = ch + 1
			pos += 2
		} else {
			out[pos] = ch
			pos += 1
		}
	}
	return out
}

func unescapeString(in []byte) []byte {
	if bytes.Count(in, []byte{1}) == 0 {
		return in
	}

	out := make([]byte, 0, len(in))
	for i := 0; i < len(in); i++ {
		if in[i] == 0x01 {
			// 01 01 -> 00
			i++
			assert(in[i] == 1 || in[i] == 2)
			out = append(out, in[i]-1)
		} else {
			out = append(out, in[i])
		}

	}

	return out
}

// order preserving encoding
func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		out = append(out, byte(v.Type))
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)        // flip the sign bit
			binary.BigEndian.PutUint64(buf[:], u) // big endian
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // null-terminated
		default:
			panic("what?")
		}
	}

	return out
}

// for input range, which can be prefix of index key
func encodeKeyPartial(out []byte, prefix uint32, vals []Value, cmp int) []byte {
	out = encodeKey(out, prefix, vals)
	if cmp == CMP_GT || cmp == CMP_LE {
		out = append(out, 0xff)
	}
	return out
}

func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)

	return out
}

func decodeKey(in []byte, out []Value) {
	decodeValues(in[4:], out)
}

func decodeValues(in []byte, out []Value) {
	for i := range out {
		switch out[i].Type {
		case TYPE_INT64:
			u := binary.BigEndian.Uint64(in[:8])
			out[i].I64 = int64(u - (1 << 63))
			in = in[8:]
		case TYPE_BYTES:
			idx := bytes.IndexByte(in, 0)
			assert(idx >= 0)
			out[i].Str = unescapeString(in[:idx])
			in = in[idx+1:]
		default:
			panic("what?")
		}
	}

	assert(len(in) == 0)
}

// check for missing columns
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	vals, err := reorderRecord(tdef, rec)
	if err != nil {
		return nil, err
	}

	err = valuesComplete(tdef, vals, n)
	if err != nil {
		return nil, err
	}
	return vals, nil
}

// extract multiple col. values
func getValues(tdef *TableDef, rec Record, cols []string) ([]Value, error) {
	vals := make([]Value, len(cols))
	for i, c := range cols {
		v := rec.Get(c)
		if v == nil {
			return nil, fmt.Errorf("missing col.: %s", tdef.Cols[i])
		}

		if v.Type != tdef.Types[slices.Index(tdef.Cols, c)] {
			return nil, fmt.Errorf("bad column type: %s", c)
		}
		vals[i] = *v
	}
	return vals, nil
}

// get a single row by primary key
func dbGet(tx *DBTX, tdef *TableDef, rec *Record) (bool, error) {
	vals, err := getValues(tdef, *rec, tdef.Indexes[0])
	if err != nil {
		return false, err
	}

	//scan operation
	sc := Scanner{
		Cmp1: CMP_GE,
		Cmp2: CMP_LE,
		Key1: Record{tdef.Indexes[0], vals},
		Key2: Record{tdef.Indexes[0], vals},
	}

	if err := dbScan(tx, tdef, &sc); err != nil || !sc.Valid() {
		return false, err
	}
	sc.Deref(rec)
	return true, nil
}

func (tx *DBTX) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(tx, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}

	return dbGet(tx, tdef, rec)
}

const TABLE_PREFIX_MIN = 100

func tableDefCheck(tdef *TableDef) error {
	// very table schema
	bad := tdef.Name == "" || len(tdef.Cols) == 0
	bad = bad || len(tdef.Cols) != len(tdef.Types)
	if bad {
		return fmt.Errorf("bad table schema: %s", tdef.Name)
	}

	// verifyin indexes
	for i, index := range tdef.Indexes {
		index, err := checkIndexCols(tdef, index)
		if err != nil {
			return err
		}
		tdef.Indexes[i] = index
	}

	return nil
}

func checkIndexCols(tdef *TableDef, index []string) ([]string, error) {
	if len(index) == 0 {
		return nil, fmt.Errorf("empty index")
	}

	seen := map[string]bool{}
	for _, c := range index {
		// check index cols
		if slices.Index(tdef.Cols, c) < 0 {
			return nil, fmt.Errorf("unknown index column: %s", c)
		}
		if seen[c] {
			return nil, fmt.Errorf("duplicated column index: %s", c)
		}

		seen[c] = true
	}

	// addin primary key to index
	for _, c := range tdef.Indexes[0] {
		if !seen[c] {
			index = append(index, c)
		}
	}
	assert(len(index) <= len(tdef.Cols))
	return index, nil
}

func (tx *DBTX) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}

	// check existing table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(tx, TDEF_TABLE, table)
	assert(err == nil)
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}

	// alllocating new prefixes
	prefix := uint32(TABLE_PREFIX_MIN)
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(tx, TDEF_META, meta)
	assert(err == nil)
	if ok {
		prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		assert(prefix > TABLE_PREFIX_MIN)
	} else {
		meta.AddStr("val", make([]byte, 4))
	}
	assert(len(tdef.Prefixes) == 0)
	for i := range tdef.Indexes {
		tdef.Prefixes = append(tdef.Prefixes, prefix+uint32(i))
	}

	// updatin next prefix
	next := prefix + uint32(len(tdef.Prefixes))
	binary.LittleEndian.PutUint32(meta.Get("val").Str, next)
	_, err = dbUpdate(tx, TDEF_META, &DBUpdateReq{Record: *meta})
	if err != nil {
		return err
	}

	// storin schema
	val, err := json.Marshal(tdef)
	assert(err == nil)
	table.AddStr("def", val)
	_, err = dbUpdate(tx, TDEF_TABLE, &DBUpdateReq{Record: *table})

	return err
}

// get table schema by naem
func getTableDef(tx *DBTX, name string) *TableDef {
	if tdef, ok := INTERNAL_TABLES[name]; ok {
		return tdef // expose internal tables
	}
	tdef := tx.db.tables[name]
	if tdef == nil {
		if tdef = getTableDefDB(tx, name); tdef != nil {
			tx.db.tables[name] = tdef
		}
	}
	return tdef
}

func getTableDefDB(tx *DBTX, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(tx, TDEF_TABLE, rec)
	assert(err == nil)
	if !ok {
		return nil
	}

	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	assert(err == nil)

	return tdef
}

type DBUpdateReq struct {
	Record  Record
	Mode    int
	Updated bool
	Added   bool
}

func nonPrimaryKeyCols(tdef *TableDef) (out []string) {
	for _, c := range tdef.Cols {
		if slices.Index(tdef.Indexes[0], c) < 0 {
			out = append(out, c)
		}
	}
	return
}

const (
	INDEX_ADD = 1
	INDEX_DEL = 2
)

// ADD OR REMOVE SECONDARY INDEX KEYS
func indexOP(tx *DBTX, tdef *TableDef, op int, rec Record) error {
	for i := 1; i < len(tdef.Indexes); i++ {
		vals, err := getValues(tdef, rec, tdef.Indexes[i])
		assert(err == nil)
		key := encodeKey(nil, tdef.Prefixes[i], vals)

		switch op {
		case INDEX_ADD:
			req := UpdateReq{Key: key, Val: nil}
			if _, err := tx.kv.Update(&req); err != nil {
				return err
			}
			assert(req.Added) // internal consistency
		case INDEX_DEL:
			deleted, err := tx.kv.Del(&DeleteReq{Key: key})
			assert(err == nil)
			assert(deleted)
		default:
			panic("unreachable")
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// add row to table
func dbUpdate(tx *DBTX, tdef *TableDef, dbreq *DBUpdateReq) (bool, error) {
	cols := slices.Concat(tdef.Indexes[0], nonPrimaryKeyCols(tdef))
	values, err := getValues(tdef, dbreq.Record, cols)
	if err != nil {
		return false, err
	}

	// insert row
	np := len(tdef.Indexes[0])
	key := encodeKey(nil, tdef.Prefixes[0], values[:np])
	val := encodeValues(nil, values[np:])
	req := UpdateReq{Key: key, Val: val, Mode: dbreq.Mode}
	if _, err := tx.kv.Update(&req); err != nil {
		return false, err
	}

	dbreq.Added, dbreq.Updated = req.Added, req.Updated

	// maintain secondary indexes
	if req.Updated && !req.Added {
		decodeValues(req.Old, values[np:])
		oldRec := Record{cols, values}
		// delete indexed keys
		err := indexOP(tx, tdef, INDEX_DEL, oldRec)
		assert(err == nil)
	}

	if req.Updated {
		if err = indexOP(tx, tdef, INDEX_ADD, dbreq.Record); err != nil {
			return false, err
		}
	}

	return req.Updated, nil
}

// addin a record
func (tx *DBTX) Set(table string, dbreq *DBUpdateReq) (bool, error) {
	tdef := getTableDef(tx, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}

	return dbUpdate(tx, tdef, dbreq)
}

func (tx *DBTX) Insert(table string, rec Record) (bool, error) {
	return tx.Set(table, &DBUpdateReq{Record: rec, Mode: MODE_INSERT_ONLY})
}
func (tx *DBTX) Update(table string, rec Record) (bool, error) {
	return tx.Set(table, &DBUpdateReq{Record: rec, Mode: MODE_UPDATE_ONLY})
}
func (tx *DBTX) Upsert(table string, rec Record) (bool, error) {
	return tx.Set(table, &DBUpdateReq{Record: rec, Mode: MODE_UPSERT})
}

// delete a record by primary key
func dbDelete(tx *DBTX, tdef *TableDef, rec Record) (bool, error) {
	vals, err := getValues(tdef, rec, tdef.Indexes[0])
	if err != nil {
		return false, err
	}

	// delete row
	req := DeleteReq{Key: encodeKey(nil, tdef.Prefixes[0], vals)}
	if deleted, _ := tx.kv.Del(&req); !deleted {
		return false, nil
	}

	for _, c := range nonPrimaryKeyCols(tdef) {
		tp := tdef.Types[slices.Index(tdef.Cols, c)]
		vals = append(vals, Value{Type: tp})
	}

	decodeValues(req.Old, vals[len(tdef.Indexes[0]):])
	err = indexOP(tx, tdef, INDEX_DEL, Record{tdef.Cols, vals})
	assert(err == nil)

	return true, nil
}

func (tx *DBTX) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(tx, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}

	return dbDelete(tx, tdef, rec)
}

func (db *DB) Open() error {
	db.kv.Path = db.Path
	db.tables = map[string]*TableDef{}

	// opening kv store
	return db.kv.Open()
}

func (db *DB) Close() {
	db.kv.Close()
}

// scanner decodes KV's into rows
// iterator for range queries
// Scanner is a wrapper for B+ Tree iterator
type Scanner struct {
	Cmp1 int
	Cmp2 int

	// range from Key 1 to key2
	Key1 Record
	Key2 Record

	// internal
	tx     *DBTX
	index  int
	tdef   *TableDef
	iter   KVIter
	keyEnd []byte
}

// within range or not
func (sc *Scanner) Valid() bool {
	return sc.iter.Valid()
}

// movin underlying B+ tree iterator
func (sc *Scanner) Next() {
	sc.iter.Next()
}

// return current row
func (sc *Scanner) Deref(rec *Record) {
	assert(sc.Valid())
	tdef := sc.tdef

	// fetch KV from iterator
	key, val := sc.iter.Deref()

	// prepare output record
	rec.Cols = slices.Concat(tdef.Indexes[0], nonPrimaryKeyCols(tdef))
	rec.Vals = rec.Vals[:0]
	for _, c := range rec.Cols {
		tp := tdef.Types[slices.Index(tdef.Cols, c)]
		rec.Vals = append(rec.Vals, Value{Type: tp})
	}

	if sc.index == 0 {
		// decode full row
		np := len(tdef.Indexes[0])
		decodeKey(key, rec.Vals[:np])
		decodeValues(val, rec.Vals[np:])
	} else {
		// decode index key
		assert(len(val) == 0)
		index := tdef.Indexes[sc.index]
		irec := Record{index, make([]Value, len(index))}

		for i, c := range index {
			irec.Vals[i].Type = tdef.Types[slices.Index(tdef.Cols, c)]
		}
		decodeKey(key, irec.Vals)

		// extract primary key
		for i, c := range tdef.Indexes[0] {
			rec.Vals[i] = *irec.Get(c)
		}

		// fetch row by primary key
		ok, err := dbGet(sc.tx, tdef, rec)
		assert(ok && err == nil)
	}
}

// check col. types
func checkTypes(tdef *TableDef, rec Record) error {
	if len(rec.Cols) != len(rec.Vals) {
		return fmt.Errorf("bad record")
	}

	for i, c := range rec.Cols {
		j := slices.Index(tdef.Cols, c)
		if j < 0 || tdef.Types[j] != rec.Vals[i].Type {
			return fmt.Errorf("bad column: %s", c)
		}
	}
	return nil
}

func dbScan(tx *DBTX, tdef *TableDef, req *Scanner) error {
	switch {
	case req.Cmp1 > 0 && req.Cmp2 < 0:
	case req.Cmp1 < 0 && req.Cmp2 > 0:
	default:
		return fmt.Errorf("bad range")
	}

	if err := checkTypes(tdef, req.Key1); err != nil {
		return err
	}
	if err := checkTypes(tdef, req.Key2); err != nil {
		return err
	}

	req.tx = tx
	req.tdef = tdef

	// select index
	isCovered := func(key []string,index []string) bool {
		return len(index) >= len(key) && slices.Equal(index[:len(key)], key)
	}

	req.index = slices.IndexFunc(tdef.Indexes, func (index []string)bool  {
		return isCovered(req.Key1.Cols, index) && isCovered(req.Key2.Cols, index)
	})
	if req.index < 0 {
		return fmt.Errorf("no index")
	}

	// encode start key
	prefix := tdef.Prefixes[req.index]
	keyStart := encodeKeyPartial(nil, prefix, req.Key1.Vals, req.Cmp1)
	keyEnd := encodeKeyPartial(nil, prefix, req.Key2.Vals, req.Cmp2)

	// seek to start key
	req.iter = tx.kv.Seek(keyStart, req.Cmp1, keyEnd, req.Cmp2)
	return nil
}

func (tx *DBTX) Scan(table string, req *Scanner) error {
	tdef := getTableDef(tx, table)
	if tdef == nil {
		return fmt.Errorf("table not found: %s", table)
	}

	return dbScan(tx, tdef, req)
}
