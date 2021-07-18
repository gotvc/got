package stores

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cadata/fsstore"
	"github.com/brendoncarroll/go-state/fs"
)

type (
	Store = cadata.Store
	ID    = cadata.ID
	Set   = cadata.Set
)

type MemSet map[ID]struct{}

func (ms MemSet) Exists(ctx context.Context, id ID) (bool, error) {
	_, exists := ms[id]
	return exists, nil
}

func (ms MemSet) Add(ctx context.Context, id ID) error {
	ms[id] = struct{}{}
	return nil
}

func (ms MemSet) Delete(ctx context.Context, id ID) error {
	delete(ms, id)
	return nil
}

func (ms MemSet) Count() int {
	return len(ms)
}

func (ms MemSet) List(ctx context.Context, first []byte, ids []cadata.ID) (int, error) {
	var n int
	for id := range ms {
		if bytes.Compare(id[:], first) < 0 {
			continue
		}
		ids[n] = id
		n++
		if n >= len(ids) {
			return n, nil
		}
	}
	return n, cadata.ErrEndOfList
}

func NewFSStore(x fs.FS, maxSize int) cadata.Store {
	return fsstore.New(x, cadata.DefaultHash, maxSize)
}
