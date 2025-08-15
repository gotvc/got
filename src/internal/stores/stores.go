package stores

import (
	"bytes"
	"context"

	"github.com/gotvc/got/src/gdat"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/cadata/fsstore"
	"go.brendoncarroll.net/state/posixfs"
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

func NewVoid() cadata.Store {
	return cadata.NewVoid(gdat.Hash, 1<<22)
}

// Reading is used for read-only operations.
type Reading interface {
	cadata.Getter
}

// Writing is used for additive copy-on-write operations.
type Writing interface {
	cadata.PostExister
}

// Writing is used for read-write operations.
type RW interface {
	Reading
	Writing
}
