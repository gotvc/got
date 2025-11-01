package volumes

import (
	"context"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/state/cadata"
)

var _ Volume = &Memory{}

type Memory struct {
	mu   sync.RWMutex
	root []byte
	s    cadata.Store
}

func NewMemory(hf cadata.HashFunc, maxSize int) *Memory {
	return &Memory{
		root: []byte{},
		s:    cadata.NewMem(hf, maxSize),
	}
}

func (v *Memory) BeginTx(ctx context.Context, tp blobcache.TxParams) (Tx, error) {
	if tp.Mutate {
		v.mu.Lock()
	} else {
		v.mu.RLock()
	}
	return &MemoryTx{vol: v, mutate: tp.Mutate}, nil
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
	for i := range dst {
		exists, err := tx.vol.s.Exists(ctx, cids[i])
		if err != nil {
			return err
		}
		dst[i] = exists
	}
	return nil
}

func (tx *MemoryTx) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return tx.vol.s.Get(ctx, cid, buf)
}

func (tx *MemoryTx) MaxSize() int {
	return tx.vol.s.MaxSize()
}

func (tx *MemoryTx) Hash(data []byte) blobcache.CID {
	return tx.vol.s.Hash(data)
}
