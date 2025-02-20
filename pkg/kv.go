package pkg


type KV struct {
	Path string
	fd   int
	tree BTree
}

func (db *KV) Open() error

func (db *KV) Get(key []byte)

