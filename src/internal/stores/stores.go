package stores

import (
	"bytes"
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/crypto/blake2b"
)

type (
	Store = cadata.Store
	ID    = blobcache.CID
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

func (ms MemSet) List(ctx context.Context, span cadata.Span, ids []blobcache.CID) (int, error) {
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

func Hash(x []byte) blobcache.CID {
	return blake2b.Sum256(x)
}

func NewMem() *cadata.MemStore {
	return cadata.NewMem(Hash, 1<<22)
}

func NewVoid() cadata.Store {
	return cadata.NewVoid(Hash, 1<<22)
}

// Reading is used for read-only operations.
type Reading interface {
	Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error)
	MaxSize() int
}

// Writing is used for additive copy-on-write operations.
type Writing interface {
	Post(ctx context.Context, data []byte) (blobcache.CID, error)
	Exists(ctx context.Context, cid blobcache.CID) (bool, error)
	MaxSize() int
}

// Writing is used for read-write operations.
type RW interface {
	Reading
	Writing
}

type RWD interface {
	Reading
	Writing
	cadata.Deleter
}

type Exister interface {
	Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error
}

func ExistsUnit(ctx context.Context, s Exister, cid blobcache.CID) (bool, error) {
	var dst [1]bool
	if err := s.Exists(ctx, []blobcache.CID{cid}, dst[:]); err != nil {
		return false, err
	}
	return dst[0], nil
}

type CopyFrom interface {
	CopyFrom(ctx context.Context, src Reading, cids []blobcache.CID, success []bool) error
}

// Copy copies a set of CIDs from src to dst.
func Copy(ctx context.Context, src Reading, dst Writing, cids ...blobcache.CID) error {
	success := make([]bool, len(cids))
	if cf, ok := dst.(CopyFrom); ok {
		if err := cf.CopyFrom(ctx, src, cids, success); err != nil {
			return err
		}
	}
	buf := make([]byte, src.MaxSize())
	for i, cid := range cids {
		if success[i] {
			continue
		}
		n, err := src.Get(ctx, cid, buf)
		if err != nil {
			return err
		}
		_, err = dst.Post(ctx, buf[:n])
		if err != nil {
			return err
		}
	}
	return nil
}
