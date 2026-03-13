package volumes

import (
	"context"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/gotvc/got/src/internal/stores"
)

var _ Volume = &Memory{}

type Memory struct {
	mu   sync.RWMutex
	root []byte
	s    stores.RW
	hf   blobcache.HashFunc
}

func NewMemory(ha blobcache.HashAlgo, maxSize int) *Memory {
	return &Memory{
		root: []byte{},
		s:    schema.NewMem(ha.HashFunc(), maxSize),
		hf:   ha.HashFunc(),
	}
}

func (v *Memory) BeginTx(ctx context.Context, tp blobcache.TxParams) (Tx, error) {
	if tp.Modify {
		v.mu.Lock()
	} else {
		v.mu.RLock()
	}
	return &MemoryTx{vol: v, mutate: tp.Modify}, nil
}

type MemoryTx struct {
	vol    *Memory
	mutate bool
}

func (tx *MemoryTx) Commit(ctx context.Context) error {
	if !tx.mutate {
		return fmt.Errorf("cannot commit a read-only transaction")
	}
	tx.vol.mu.Unlock()
	return nil
}

func (tx *MemoryTx) Abort(ctx context.Context) error {
	if tx.mutate {
		tx.vol.mu.Unlock()
	} else {
		tx.vol.mu.RUnlock()
	}
	return nil
}

func (tx *MemoryTx) Save(ctx context.Context, root []byte) error {
	if !tx.mutate {
		return fmt.Errorf("cannot save a read-only transaction")
	}
	tx.vol.root = append(tx.vol.root[:0], root...)
	return nil
}

func (tx *MemoryTx) Load(ctx context.Context, dst *[]byte) error {
	*dst = make([]byte, len(tx.vol.root))
	copy(*dst, tx.vol.root)
	return nil
}

func (tx *MemoryTx) Post(ctx context.Context, data []byte) (cid blobcache.CID, err error) {
	return tx.vol.s.Post(ctx, data)
}

func (tx *MemoryTx) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return tx.vol.s.Exists(ctx, cids, dst)
}

func (tx *MemoryTx) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return tx.vol.s.Get(ctx, cid, buf)
}

func (tx *MemoryTx) MaxSize() int {
	return tx.vol.s.MaxSize()
}

func (tx *MemoryTx) Hash(data []byte) blobcache.CID {
	return tx.vol.hf(nil, data)
}
