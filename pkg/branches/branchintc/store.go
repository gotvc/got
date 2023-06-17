package branchintc

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

var _ cadata.Store = &Store{}

type storeHook = func(verb Verb, next func() error) error

type Store struct {
	inner cadata.Store
	hook  storeHook
}

func newStore(x cadata.Store, hook storeHook) *Store {
	return &Store{inner: x, hook: hook}
}

func (s *Store) Post(ctx context.Context, data []byte) (ret cadata.ID, err error) {
	err = s.hook(Verb_PostBlob, func() error {
		var err error
		ret, err = s.inner.Post(ctx, data)
		return err
	})
	return ret, err
}

func (s *Store) Delete(ctx context.Context, id cadata.ID) error {
	return s.hook(Verb_DeleteBlob, func() error {
		return s.inner.Delete(ctx, id)
	})
}

func (s *Store) Get(ctx context.Context, id cadata.ID, buf []byte) (n int, err error) {
	err = s.hook(Verb_GetBlob, func() error {
		var err error
		n, err = s.inner.Get(ctx, id, buf)
		return err
	})
	return n, err
}

func (s *Store) Exists(ctx context.Context, id cadata.ID) (ret bool, err error) {
	err = s.hook(Verb_ExistsBlob, func() error {
		var err error
		ret, err = cadata.Exists(ctx, s.inner, id)
		return err
	})
	return ret, err
}

func (s *Store) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (ret int, err error) {
	err = s.hook(Verb_ListBlob, func() error {
		var err error
		ret, err = s.inner.List(ctx, span, ids)
		return err
	})
	return ret, err
}

func (s *Store) Hash(x []byte) cadata.ID {
	return s.inner.Hash(x)
}

func (s *Store) MaxSize() int {
	return s.inner.MaxSize()
}
