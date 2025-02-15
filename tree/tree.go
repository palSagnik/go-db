package tree

import (
	"bytes"
	"encoding/binary"

	assert "github.com/palSagnik/go-db/internal/assert"
)

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
	nodeMax := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert.Assert(nodeMax <= BTREE_PAGE_SIZE, "max node size must be less than BTREE page size")
}

/*
Node includes
|<-- HEADER -->|
| type | nkeys |  pointers  |   offsets   | key-values | unused |
| 2B   |  2B   | nkeys * 8B | nkeys * 2B  |    ...     |

Key-Value Pair includes
| klen | vlen | key | val |
|  2B  |  2B  | ... | ... |


btype(), nkeys(): 			Read the fixed-size header.
setHeader(type, nkeys): 	Write the fixed-size header.
getPtr(n), setPtr(n, ptr):  Read and write the pointer array (for internal nodes).
getOffset(n): 				Read the offsets array to locate the nth key in O(1).
kvPos(n): 					Return the position of the nth key using getOffset().
getKey(n): 					Get the nth key data as a slice.
getVal(n): 					Get the nth value data as a slice (for leaf nodes).
*/

type BNode []byte

// header
// node type
const (
	BNODE_NODE = 1 // internal node which has no values
	BNODE_LEAF = 2 // leaf node which has all values
)

// getters
// btype returns the type of BNode it is
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

// nkeys returns the number of keys BNode has
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

// setter
// setHeader takes btype, nkeys and sets the header of the node
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], btype)
}

// pointers
// getPtr returns the pointer at the given the index
func (node BNode) getPtr(idx uint16) uint64 {
	assert.Assert(idx < node.nkeys(), "index must not exceed maximum number of keys")

	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

// setPtr sets the value of the pointer at the given index
func (node BNode) setPtr(idx uint16, val uint64) {
	assert.Assert(idx < node.nkeys(), "index must not exceed maximum number of keys")

	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// offset List
func offsetPos(node BNode, idx uint16) uint16 {
	assert.Assert(1 <= idx && idx <= node.nkeys(), "value of index must lie within bounds")
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	assert.Assert(1 <= idx && idx <= node.nkeys(), "value of index must lie within bounds")

	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	assert.Assert(idx < node.nkeys(), "index must not exceed maximum number of keys")

	// header + size of nkeys + size of offsets + kv-offset
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	assert.Assert(idx < node.nkeys(), "index must not exceed maximum number of keys")

	kvpos := node.kvPos(idx)
	keylen := binary.LittleEndian.Uint16(node[kvpos:])

	// klen + vlen = 4
	return node[kvpos+4:][:keylen]
}

func (node BNode) getVal(idx uint16) []byte {
	assert.Assert(idx < node.nkeys(), "index must not exceed maximum number of keys")

	kvpos := node.kvPos(idx)
	keylen := binary.LittleEndian.Uint16(node[kvpos:])
	vlen := binary.LittleEndian.Uint16(node[kvpos+2:])

	return node[kvpos + HEADER + keylen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {

	// set the header
	new.setHeader(BNODE_LEAF, old.nkeys() + 1)
	nodeAppendRange(new, old, 0, 0, idx)		 // keys before idx
	nodeAppendKV(new, idx, 0, key, val)          // new key
	nodeAppendRange(new, old, idx, idx + 1, old.nkeys() - idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {

	// set the header
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)		 // keys before idx
	nodeAppendKV(new, idx, 0, key, val)          // new key
	nodeAppendRange(new, old, idx, idx + 1, old.nkeys() - (idx + 1))
}

// nodeAppendKV inserts a new KV into the given index
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {

	// ptrs
	new.setPtr(idx, ptr)

	// position of KV-Pair
	pos := new.kvPos(idx)

	// key length and val length
	binary.LittleEndian.PutUint16(new[pos + 0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos + 2:], uint16(len(val)))

	// copying key and value
	copy(new[pos + HEADER:], key)
	copy(new[pos + HEADER + uint16(len(key)):], val)

	// set offset
	new.setOffset(idx + 1, new.getOffset(idx) + 4 + uint16(len(key) + len(val)))
}

// nodeAppendRange copies multiple KVs into the position from the old node
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew + i, srcOld + i
		nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getVal(src))
	}
}

// TODO(sagnik): Binary Search
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()

	var i uint16
	for i = 0; i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			return i
		}
		if cmp > 0 {
			return i - 1
		}
	}
	return i - 1
}

// split an oversized node into 2 nodes
func nodeSplitInTwo(left BNode, right BNode, old BNode) {
	assert.Assert(old.nkeys() >= 2, "there must be atleast 2 keys")

	// left half
	nleft := old.nkeys() / 2
	leftBytes := func () uint16 {
		return HEADER + 8 * nleft + 2 * nleft + old.getOffset(nleft)
	}
	for leftBytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	assert.Assert(nleft >= 1, "there must be atleast 1 key")

	// right half
	rightBytes := func () uint16 {
		return old.nbytes() - leftBytes() + 4
	}
	for rightBytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	assert.Assert(nleft < old.nkeys(), "left half size should not exceed the total")

	nright := old.nkeys() - nleft

	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)

	// NOTE: we have only made sure that the right half is of proper size,
	// 		 the left half might be still too big

	assert.Assert(right.nbytes() <= BTREE_PAGE_SIZE, "the right half size is less than page size")
}

func nodeSplitInThree(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}    // not split
	}

	left := BNode(make([]byte, 2 * BTREE_PAGE_SIZE))
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplitInTwo(left, right, old)

	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 Nodes
	}

	// left still bigger
	leftLeft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplitInTwo(leftLeft, middle, left)
	assert.Assert(leftLeft.nbytes() <= BTREE_PAGE_SIZE, "all nodes have been split to appropriate sizes")

	return 2, [3]BNode{leftLeft, middle, right} // 3 Nodes
}







type BTree struct {
	root uint64              // pointer (a nonzero page number)
	get  func(uint64) []byte // dereference a pointer
	new  func([]byte) uint64 // allocate a new page
	del  func(uint64)        // deallocate a page
}