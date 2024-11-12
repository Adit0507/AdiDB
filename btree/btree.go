package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const HEADER = 4
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func assert(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1max <= BTREE_PAGE_SIZE)
}

// in memory data type
type BNode []byte

type BTree struct {
	root uint64
	get  func(uint64) []byte // dereferecne a pointer -- reads a page from disk
	new  func([]byte) uint64 //alocates & writes a new page
	del  func(uint64)        //delocate page
}

const (
	BNODE_NODE = 1 //internal nodes without values
	BNODE_LEAF = 2 //leaf nodes with values
)

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())

	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())

	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])

	return node[pos+4:][:klen]
}
func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// copies a KV pair
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	new.setPtr(idx, ptr)

	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)

	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// copies multiple KV's into posiiton from old node
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	assert(srcOld+n <= old.nkeys())
	assert(dstNew+n <= new.nkeys())
	if n == 0 {
		return
	}

	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+1))
	}

	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}

	// kv's
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new[new.kvPos(dstNew):], old[begin:end])
}

// addin a new key to leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

func nodeReplaceKid1ptr(new BNode, old BNode, idx uint16, ptr uint64) {
	copy(new, old[:old.nbytes()])
	new.setPtr(idx, ptr) // only the pointer is changed
}

func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	if inc == 1 && bytes.Equal(kids[0].getKey(0), old.getKey(idx)) {
		nodeReplaceKid1ptr(new, old, idx, tree.new(kids[0]))
		return
	}

	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)

	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// split a big node into 2
func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2)

	nleft := old.nkeys() / 2

	left_bytes := func() uint16 {
		return HEADER + 8*nleft + 2*nleft + old.getOffset(nleft)
	}

	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	assert(nleft >= 1)

	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + HEADER
	}
	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	assert(nleft < old.nkeys())
	nright := old.nkeys() - nleft

	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)

	assert(right.nbytes() <= BTREE_PAGE_SIZE)
}

// splits an oversized node
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} //wont split
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)

	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	mostLeft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(mostLeft, middle, left)
	assert(mostLeft.nbytes() <= BTREE_PAGE_SIZE)

	return 3, [3]BNode{mostLeft, middle, right}
}

const (
	MODE_UPSERT      = 0 //insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 //add only new keys
)

type UpdateReq struct {
	tree    *BTree
	Added   bool // new key
	Updated bool
	Old     []byte //value before update
	Key     []byte
	Val     []byte
	Mode    int
}

type DeleteReq struct {
	tree *BTree
	Key  []byte
	Old  []byte
}

// tree insertion- inserts a KV into a node
func treeInsert(req *UpdateReq, node BNode) BNode {
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	idx := nodeLookupLE(node, req.Key)
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(req.Key, node.getKey(idx)) {
			// updating the key
			leafUpdate(new, node, idx, req.Key, req.Val)
		} else {
			leafInsert(new, node, idx+1, req.Key, req.Val)
		}

	case BNODE_NODE:
		nodeInsert(req, new, node, idx)
	default:
		panic("bad node!")
	}

	return new
}

// KV insertion to an internal node
func nodeInsert(req *UpdateReq, new BNode, node BNode, idx uint16) BNode {
	kptr := node.getPtr(idx)

	// insertion to kid node
	updated := treeInsert(req, req.tree.get(kptr))
	if len(updated) == 0 {
		return BNode{}
	}

	nsplit, split := nodeSplit3(updated)
	// deallocate kid node
	req.tree.del(kptr)
	nodeReplaceKidN(req.tree, new, node, idx, split[:nsplit]...)

	return new
}

func checkLimit(key []byte, val []byte) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}

	if len(key) > BTREE_MAX_KEY_SIZE {
		return errors.New("key too long")
	}
	if len(key) > BTREE_MAX_VAL_SIZE {
		return errors.New("value too long")
	}

	return nil
}

func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

// tree deletion
// remove key from leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

// mergin 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
	assert(new.nbytes() <= BTREE_PAGE_SIZE)
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sibling := BNode(tree.get(node.getPtr(idx - 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling //left
		}
	}

	if idx+1 < node.nkeys() {
		sibling := BNode(tree.get(node.getPtr(idx + 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}

	return 0, BNode{}
}

func treeDelete(req *DeleteReq, node BNode) BNode {
	idx := nodeLookupLE(node, req.Key)

	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(req.Key, node.getKey(idx)) {
			return BNode{} // not found
		}
		// delete the key in the leaf
		req.Old = node.getVal(idx)
		new := BNode(make([]byte, BTREE_PAGE_SIZE))
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return nodeDelete(req, node, idx)
	default:
		panic("bad node")
	}
}

func nodeDelete(req *DeleteReq, node BNode, idx uint16) BNode {
	tree := req.tree
	
	kptr := node.getPtr(idx)
	updated := treeDelete(req, tree.get(kptr))
	if len(updated) == 0 {
		return BNode{}
	}
	tree.del(kptr)

	new := BNode(make([]byte, BTREE_PAGE_SIZE))

	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))

	case mergeDir == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && idx == 0)
		new.setHeader(BNODE_NODE, 0)
	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceKidN(tree, new, node, idx, updated)
	}

	return new
}

func (tree *BTree) Upsert(key []byte, val []byte) (bool, error) {
	return tree.Update(&UpdateReq{Key: key, Val: val})
}

func (tree *BTree) Update(req *UpdateReq) (bool, error) {
	if err := checkLimit(req.Key, req.Val); err != nil {
		return false, err
	}

	if tree.root == 0 {
		// create first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)

		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, req.Key, req.Val)
		tree.root = tree.new(root)
		req.Added = true
		req.Updated = true
		return true, nil
	}

	req.tree = tree
	updated := treeInsert(req, tree.get(tree.root))
	if len(updated) == 0 {
		return false, nil
	}

	nsplit, split := nodeSplit3(updated)
	tree.del(tree.root)
	if nsplit > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}

	return true, nil
}

func (tree *BTree) Delete(req *DeleteReq) (bool, error) {
	if err := checkLimit(req.Key, nil); err != nil {
		return false, err
	}

	if tree.root == 0 {
		return false, nil
	}

	req.tree = tree
	updated := treeDelete(req, tree.get(tree.root))
	if len(updated) == 0 {
		return false, nil
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}

	return true, nil
}

func nodeGetKey(tree *BTree, node BNode, key []byte) ([]byte, bool) {
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			return node.getVal(idx), true
		} else {
			return nil, false
		}
	case BNODE_NODE:
		return nodeGetKey(tree, tree.get(node.getPtr(idx)), key)

	default:
		panic("bad node")
	}
}

func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}

	return nodeGetKey(tree, tree.get(tree.root), key)
}
