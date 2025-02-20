package pkg

import (
	"bytes"
	"encoding/binary"
	"errors"

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
| 2B   |  2B   | nkeys * 8B | nkeys * 2B  |    ...     |        |

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

	return node[kvpos+HEADER+keylen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {

	// set the header
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx) // keys before idx
	nodeAppendKV(new, idx, 0, key, val)  // new key
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {

	// set the header
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx) // keys before idx
	nodeAppendKV(new, idx, 0, key, val)  // new key
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

// nodeAppendKV inserts a new KV into the given index
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {

	// ptrs
	new.setPtr(idx, ptr)

	// position of KV-Pair
	pos := new.kvPos(idx)

	// key length and val length
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))

	// copying key and value
	copy(new[pos+HEADER:], key)
	copy(new[pos+HEADER+uint16(len(key)):], val)

	// set offset
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

// nodeAppendRange copies multiple KVs into the position from the old node
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
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
	leftBytes := func() uint16 {
		return HEADER + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for leftBytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	assert.Assert(nleft >= 1, "there must be atleast 1 key")

	// right half
	rightBytes := func() uint16 {
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
		return 1, [3]BNode{old} // not split
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
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

// B+Tree structure
type BTree struct {
	// pointer (a nonzero page number)
	root uint64

	// callbacks for managing on-disk pages
	// dereference a pointer
	get func(uint64) []byte

	// allocate a new page
	new func([]byte) uint64

	// deallocate a page
	del func(uint64)
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {

	// extra size allows it to exceed 1 page temporarily
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	// place to insert the key
	idx := nodeLookupLE(node, key)

	// check node type
	switch node.btype() {

	// leaf node
	case BNODE_LEAF:
		if bytes.Equal(key, new.getKey(idx)) {
			leafUpdate(new, node, idx, key, val) // found, update it
		} else {
			leafInsert(new, node, idx+1, key, val) // not found, insert it
		}

	// internal node, walk into child node
	case BNODE_NODE:
		// recursive iteration to the child node
		cptr := node.getPtr(idx)
		cnode := treeInsert(tree, tree.get(cptr), key, val)

		// split after insertion
		nsplit, split := nodeSplitInThree(cnode)

		// deallocate the node
		tree.del(cptr)

		// update the child links
		nodeReplaceChildN(tree, new, node, idx, split[:nsplit]...)
	}
	return new
}

func nodeReplaceChildN(tree *BTree, new BNode, old BNode, idx uint16, children ...BNode) {
	inc := uint16(len(children))
	new.setHeader(BNODE_NODE, old.nkeys()-inc+1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range children {
		nodeAppendKV(new, inc+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// insert a new key or update an existing one
func (tree *BTree) Insert(key []byte, val []byte) error {
	// 1. check the length limit imposed by BTREE_MAX_VAL_SIZE, BTREE_MAX_KEY_SIZE
	if err := assert.CheckLimit(key, BTREE_MAX_KEY_SIZE, val, BTREE_MAX_VAL_SIZE); err != nil {
		return err
	}

	// 2. create the root node
	if tree.root == 0 {

		// use the concept of sentinel value, where at the first node an empty key is inserted to eliminate edge cases
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)

		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, nil, nil)
		tree.root = tree.new(root)
		return nil
	}

	// 3. insert key
	node := treeInsert(tree, tree.get(tree.root), key, val)

	// 4. grow the tree if root is split
	nsplit, split := nodeSplitInThree(node)
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
	return nil
}

// TODO(sagnik): Deletion
// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {

	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		leafDelete(new, node, idx)

	case BNODE_NODE:
		new = internalNodeDelete(tree, node, idx, key)
	}

	return new
}

// leafDelete removes a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {

	// changing number of nkeys
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)                     // copy the kvs before idx
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx-1) // copy the keys after idx, skip idx
}

// internalNodeDelete removes a key from an internal node
func internalNodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// search by recursion into child
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr)

	// check for merging
	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	mergeChoice, sibling := shouldMerge(tree, node, updated, idx)

	switch {

	// left
	case mergeChoice == -1:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplaceTwoChild(new, node, idx-1, tree.new(merged), merged.getKey(0))

	// right
	case mergeChoice == 1:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplaceTwoChild(new, node, idx+1, tree.new(merged), merged.getKey(0))

	// 1 empty child no sibling
	// parent becomes empty too
	case mergeChoice == 0 && updated.nkeys() == 0:
		assert.Assert(node.nkeys() == 1 && idx == 0, "an empty child with no siblings")
		new.setHeader(BNODE_NODE, 0)

	// no merge
	case mergeChoice == 0 && updated.nkeys() > 0:
		nodeReplaceChildN(tree, new, node, idx, updated)
	}

	return new
}

// should the updated child be merged with sibling
func shouldMerge(tree *BTree, node BNode, updatedNode BNode, idx uint16) (int, BNode) {
	if updatedNode.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	// check for left
	if idx > 0 {
		sibling := BNode(tree.get(node.getPtr(idx - 1)))
		mergedBytes := sibling.nbytes() + updatedNode.nbytes() - HEADER
		if mergedBytes <= BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}

	// check for right
	if idx+1 < node.nkeys() {
		sibling := BNode(tree.get(node.getPtr(idx + 1)))
		mergedBytes := sibling.nbytes() + updatedNode.nbytes() - HEADER
		if mergedBytes <= BTREE_PAGE_SIZE {
			return 1, sibling // right
		}
	}
	return 0, BNode{}
}

func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())

	nodeAppendRange(new, left, 0, 0, left.nkeys())              // copy from left node
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys()) // copy from right node
}

func nodeReplaceTwoChild(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)

	nodeAppendRange(new, old, 0, 0, idx)                       // copy kvs before idx
	nodeAppendKV(new, idx, ptr, key, old.getVal(idx))          // copy the merged child at idx
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-idx-2) // copy everything after the two replaced keys
}

// delete a key and returns whether the key was there
func (tree *BTree) Delete(key []byte) (bool, error) {

	// 1. check if tree exists
	if tree.root == 0 {
		return false, errors.New("the tree is empty")
	}

	// 2. attempt deletion
	oldRoot := tree.get(tree.root)
	newRoot := treeDelete(tree, oldRoot, key)

	// 3. check if delete returned same nodes
	// if yes, then it means no key was deleted
	if bytes.Equal(oldRoot, newRoot) {
		return false, errors.New("no key was found")
	}

	// 4. check if root needs to shrink
	if newRoot.nkeys() == 1 && newRoot.btype() == BNODE_NODE {
		// If root is an internal node with only one child, make that child the new root
		tree.del(tree.root)
		tree.root = newRoot.getPtr(0)
	} else {
		// Otherwise, just update the root with the new node
		tree.del(tree.root)
		tree.root = tree.new(newRoot)
	}

	return true, nil
}
