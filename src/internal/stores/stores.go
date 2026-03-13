package stores

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"golang.org/x/crypto/blake2b"
)

type (
	CID = blobcache.CID
)

type Set interface {
	Exists(context.Context, CID) (bool, error)
	Add(context.Context, CID) error
}

type MemSet map[CID]struct{}

func (ms MemSet) Exists(ctx context.Context, id CID) (bool, error) {
	_, exists := ms[id]
	return exists, nil
}

func (ms MemSet) Add(ctx context.Context, id CID) error {
	ms[id] = struct{}{}
	return nil
}

func (ms MemSet) Delete(ctx context.Context, id CID) error {
	delete(ms, id)
	return nil
}

func (ms MemSet) Count() int {
	return len(ms)
}

func Hash(x []byte) blobcache.CID {
	return blake2b.Sum256(x)
}

const MaxSize = 1 << 21

func NewMem() *schema.MemStore {
	return schema.NewMem(blobcache.HashAlgo_BLAKE2b_256.HashFunc(), 1<<22)
}

func NewMemSize(s int) *schema.MemStore {
	return schema.NewMem(blobcache.HashAlgo_BLAKE2b_256.HashFunc(), 1<<22)
}

// RO is used for read-only operations.
type RO interface {
	Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error)
	Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error
	MaxSize() int
}

// WO is used for additive copy-on-write operations.
type WO interface {
	Post(ctx context.Context, data []byte) (blobcache.CID, error)
	Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error
	MaxSize() int
}

// Writing is used for read-write operations.
type RW interface {
	RO
	WO
}

type RWD interface {
	RO
	WO
	Delete(ctx context.Context, cids []blobcache.CID) error
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
	CopyFrom(ctx context.Context, src RO, cids []blobcache.CID, success []bool) error
}

// Copy copies a set of CIDs from src to dst.
func Copy(ctx context.Context, src RO, dst WO, cids ...blobcache.CID) error {
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
