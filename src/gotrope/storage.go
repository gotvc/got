package gotrope

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state/cadata"
)

type Storage[R any] interface {
	Get(ctx context.Context, ref R, buf []byte) (int, error)
	MaxSize() int
	MarshalRef(R) []byte
	ParseRef([]byte) (R, error)
}

type WriteStorage[R any] interface {
	Storage[R]
	Post(ctx context.Context, data []byte) (R, error)
}

type storage struct {
	s stores.Reading
}

func NewStorage(s cadata.Getter) Storage[blobcache.CID] {
	return storage{s}
}

func (s storage) Get(ctx context.Context, ref blobcache.CID, buf []byte) (int, error) {
	return s.s.Get(ctx, ref, buf)
}

func (s storage) MarshalRef(ref blobcache.CID) []byte {
	return ref[:]
}

func (s storage) ParseRef(x []byte) (blobcache.CID, error) {
	if len(x) != blobcache.CIDSize {
		return blobcache.CID{}, fmt.Errorf("gotrope: invalid ref length: %d", len(x))
	}
	return blobcache.CID(x[:]), nil
}

func (s storage) MaxSize() int {
	return s.s.MaxSize()
}

type writeStore struct {
	storage
	s cadata.Store
}

func NewWriteStorage(s cadata.Store) WriteStorage[blobcache.CID] {
	return writeStore{
		storage: storage{s},
		s:       s,
	}
}

func (s writeStore) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	return s.s.Post(ctx, data)
}
