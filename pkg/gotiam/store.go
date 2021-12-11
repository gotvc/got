package gotiam

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

var _ cadata.Store = &Store{}

type Store struct {
	inner cadata.Store
	check func(bool, string) error
}

func newStore(x cadata.Store, check func(bool, string) error) *Store {
	return &Store{inner: x}
}

func (s *Store) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	if err := s.check(true, "POST"); err != nil {
		return cadata.ID{}, err
	}
	return s.inner.Post(ctx, data)
}

func (s *Store) Delete(ctx context.Context, id cadata.ID) error {
	if err := s.check(true, "DELETE"); err != nil {
		return err
	}
	return s.inner.Delete(ctx, id)
}

func (s *Store) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	if err := s.check(false, "GET"); err != nil {
		return -1, err
	}
	return s.inner.Get(ctx, id, buf)
}

func (s *Store) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	if err := s.check(false, "EXISTS"); err != nil {
		return false, err
	}
	return s.inner.Exists(ctx, id)
}

func (s *Store) List(ctx context.Context, prefix []byte, ids []cadata.ID) (int, error) {
	if err := s.check(false, "LIST"); err != nil {
		return -1, err
	}
	return s.inner.List(ctx, prefix, ids)
}

func (s *Store) Hash(x []byte) cadata.ID {
	return s.inner.Hash(x)
}

func (s *Store) MaxSize() int {
	return s.inner.MaxSize()
}
