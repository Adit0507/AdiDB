package btree_iter

import (
	"bytes"
	"github.com/Adit0507/AdiDB/btree"
)

type BIter struct {
	tree *btree.BTree
	path []btree.BNode
	pos  []uint16
}

// movin backward & forward
func (iter *BIter) Next() {
	iterNext(iter, len(iter.path)-1)
}

func iterIsFirst(iter *BIter) bool {
	for _, pos := range iter.pos {
		if pos != 0 {
			return false
		}
	}
	return true
}

func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]-- //move within node
	} else if level > 0 {
		iterPrev(iter, level-1) //move to sibling noe
	} else {
		panic("unreachable")
	}

	if level+1 < len(iter.pos) {
		node := iter.path[level]
		kid := BNode(iter.tree.get(node.getPtr(iter.pos[level])))
		iter.path[level+1] = kid
		iter.pos[level+1] = kid.nkeys() - 1
	}
}

func (iter *BIter) Prev() {
	if !iterIsFirst(iter) {
		iterPrev(iter, len(iter.path)-1)
	}
}

func iterNext(iter *BIter, level int) {
	if iter.pos[level]+1 < iter.path[level].nkeys() {
		iter.pos[level]++ //move within node
	} else if level > 0 {
		iterNext(iter, level-1) //move to sibling node
	} else {
		iter.pos[len(iter.pos)-1]++ //past last key
		return
	}
	if level+1 < len(iter.pos) { //update child node
		node := iter.path[level]
		kid := btree.BNode(iter.tree.get(node.getPtr(iter.pos[level])))
		iter.path[level+1] = kid
		iter.pos[level+1] = 0
	}
}

// find closest position that is less or equal to input key
func (tree BTreeWrap) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.root; ptr != 0; {
		node := (tree.get(ptr))
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		ptr = node.getPtr(idx)
	}
	return iter
}

func assert(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}

// get current KV pair
func (iter *BIter) Deref() ([]byte, []byte) {
	assert(iter.Valid())
	last := len(iter.path) - 1
	node := iter.path[last]
	pos := iter.pos[last]

	return node.getKey(pos), node.getVal(pos)
}

func iterIsEnd(iter *BIter) bool {
	last := len(iter.path) - 1
	return last < 0 || iter.pos[last] >= uint16(iter.path[last].nkeys)
}

// precondition of Dref()
func (iter *BIter) Valid() bool {
	return !(iterIsFirst(iter) || iterIsEnd(iter))
}

const (
	CMP_GE = +3 // >=
	CMP_GT = +2 // >
	CMP_LT = -2 // <
	CMP_LE = -3 // <=
)

func cmpOk(key []byte, cmp int, ref []byte) bool {
	r := bytes.Compare(key, ref)

	switch cmp {
	case CMP_GE:
		return r >= 0

	case CMP_GT:
		return r > 0
	case CMP_LE:
		return r <= 0
	case CMP_LT:
		return r < 0
	default:
		panic("what>")
	}
}

type BTreeWrap struct{
	*btree.BTree
}

func (tree BTreeWrap) Seek(key []byte, cmp int) *BIter {
	iter := tree.SeekLE(key)
	assert(iterIsFirst(iter) || !iterIsEnd(iter))
	if cmp != CMP_LE {
		cur := []byte(nil)
		if !iterIsFirst(iter) {
			cur, _ = iter.Deref()
		}

		if len(key) == 0 || !cmpOk(cur, cmp, key) {
			if cmp > 0 {
				iter.Next()
			} else {
				iter.Prev()
			}
		}
	}

	if iter.Valid() {
		cur, _ := iter.Deref()
		assert(cmpOk(cur, cmp, key))
	}

	return iter
}
