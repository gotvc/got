package gotauthz

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

var _ cadata.Store = &Store{}

type Store struct {
	inner cadata.Store
	check checkFn
}

func newStore(x cadata.Store, check checkFn) *Store {
	return &Store{inner: x, check: check}
}

func (s *Store) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	if err := s.check(Verb_PostBlob); err != nil {
		return cadata.ID{}, err
	}
	return s.inner.Post(ctx, data)
}

func (s *Store) Delete(ctx context.Context, id cadata.ID) error {
	if err := s.check(Verb_DeleteBlob); err != nil {
		return err
	}
	return s.inner.Delete(ctx, id)
}

func (s *Store) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	if err := s.check(Verb_GetBlob); err != nil {
		return -1, err
	}
	return s.inner.Get(ctx, id, buf)
}

func (s *Store) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	if err := s.check(Verb_ExistsBlob); err != nil {
		return false, err
	}
	return cadata.Exists(ctx, s.inner, id)
}

func (s *Store) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (int, error) {
	if err := s.check(Verb_ListBlob); err != nil {
		return -1, err
	}
	return s.inner.List(ctx, span, ids)
}

func (s *Store) Hash(x []byte) cadata.ID {
	return s.inner.Hash(x)
}

func (s *Store) MaxSize() int {
	return s.inner.MaxSize()
}
