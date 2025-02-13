package tree

import (
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
| type | nkeys |  pointers  |   offsets   | key-values | unused |
| 2B   |  2B   | nkeys * 8B | nkeys * 2B  |    ...     |

Key-Value Pair includes
| klen | vlen | key | val |
|  2B  |  2B  | ... | ... |
*/

type BTree struct {
	root uint64              // pointer (a nonzero page number)
	get  func(uint64) []byte // dereference a pointer
	new  func([]byte) uint64 // allocate a new page
	del  func(uint64)        // deallocate a page
}

type BNode []byte

// Header
const (
	BNODE_NODE = 1 // internal node which has no values
	BNODE_LEAF = 2 // leaf node which has all values
)

// node.btype() returns the type of BNode it is
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

// node.nkeys() returns the number of keys BNode has
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], btype)
}
