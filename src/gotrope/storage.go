package gotrope

import (
	"context"

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
	s cadata.Getter
}

func NewStorage(s cadata.Getter) Storage[cadata.ID] {
	return storage{s}
}

func (s storage) Get(ctx context.Context, ref cadata.ID, buf []byte) (int, error) {
	return s.s.Get(ctx, ref, buf)
}

func (s storage) MarshalRef(ref cadata.ID) []byte {
	return ref[:]
}

func (s storage) ParseRef(x []byte) (cadata.ID, error) {
	return cadata.IDFromBytes(x), nil
}

func (s storage) MaxSize() int {
	return s.s.MaxSize()
}

type writeStore struct {
	storage
	s cadata.Store
}

func NewWriteStorage(s cadata.Store) WriteStorage[cadata.ID] {
	return writeStore{
		storage: storage{s},
		s:       s,
	}
}

func (s writeStore) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	return s.s.Post(ctx, data)
}
