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

type BTree struct {
	root uint64              // pointer (a nonzero page number)
	get  func(uint64) []byte // dereference a pointer
	new  func([]byte) uint64 // allocate a new page
	del  func(uint64)        // deallocate a page
}

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

	return node[kvpos+4+keylen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// TODO(sagnik): Binary Search
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

func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {

	// set the header
	new.setHeader(BNODE_LEAF, old.nkeys() + 1)
	nodeAppendRange(new, old, 0, 0, idx)		 // keys before idx
	nodeAppendKV(new, idx, 0, key, val)          // new key
	nodeAppendRange(new, old, idx, idx + 1, old.nkeys() - idx)
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
	copy(new[pos + 4:], key)
	copy(new[pos + 4 + uint16(len(key)):], val)

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