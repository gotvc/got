package branchintc

import (
	"context"

	"github.com/gotvc/got/pkg/branches"
)

var _ branches.Space = &Space{}

type Space struct {
	inner branches.Space
	hook  Hook
}

func New(x branches.Space, hook Hook) *Space {
	return &Space{
		inner: x,
		hook:  hook,
	}
}

func (s *Space) Create(ctx context.Context, k string, cfg branches.Config) (ret *branches.Info, err error) {
	err = s.hook("CREATE", k, func() error {
		b, err := s.inner.Create(ctx, k, cfg)
		ret = b
		return err
	})
	return ret, err
}

func (s *Space) Delete(ctx context.Context, k string) (err error) {
	err = s.hook("DELETE", k, func() error {
		return s.inner.Delete(ctx, k)
	})
	return err
}

func (s *Space) Get(ctx context.Context, k string) (ret *branches.Info, err error) {
	err = s.hook("GET", k, func() error {
		b, err := s.inner.Get(ctx, k)
		ret = b
		return err
	})
	return ret, err
}

func (s *Space) Set(ctx context.Context, k string, md branches.Config) (err error) {
	err = s.hook("SET", k, func() error {
		return s.inner.Set(ctx, k, md)
	})
	return err
}

func (s *Space) List(ctx context.Context, span branches.Span, limit int) (ret []string, err error) {
	err = s.hook("LIST", "", func() error {
		var err error
		ret, err = s.inner.List(ctx, span, limit)
		return err
	})
	return ret, err
}

func (s *Space) Open(ctx context.Context, name string) (*branches.Volume, error) {
	v, err := s.inner.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return s.wrapVolume(v, name), nil
}

func (s *Space) wrapVolume(x *branches.Volume, name string) *branches.Volume {
	storeHook := func(v Verb, next func() error) error {
		return s.hook(v, name, next)
	}
	return &branches.Volume{
		Cell: newCell(x.Cell, func(verb Verb, next func() error) error {
			return s.hook(verb, name, next)
		}),
		VCStore:  newStore(x.VCStore, storeHook),
		FSStore:  newStore(x.FSStore, storeHook),
		RawStore: newStore(x.RawStore, storeHook),
	}
}
