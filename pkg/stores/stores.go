package stores

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cadata/fsstore"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gdat"
)

type (
	Store = cadata.Store
	ID    = cadata.ID
	Set   = cadata.Set
)

var _ cadata.Set = MemSet{}

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

func (ms MemSet) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (int, error) {
	var n int
	for id := range ms {
		if n >= len(ids) {
			break
		}
		c := span.Compare(id, func(a, b ID) int {
			return bytes.Compare(a[:], b[:])
		})
		if c > 0 {
			continue
		} else if c < 0 {
			break
		}
		ids[n] = id
		n++
	}
	return n, nil
}

func NewFSStore(x posixfs.FS, maxSize int) cadata.Store {
	return fsstore.New(x, gdat.Hash, maxSize)
}

func NewMem() cadata.Store {
	return cadata.NewMem(gdat.Hash, 1<<22)
}
