package branchintc

import (
	"context"

	"github.com/gotvc/got/src/branches"
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
	err = s.hook(ctx, "CREATE", k, func(ctx context.Context) error {
		b, err := s.inner.Create(ctx, k, cfg)
		ret = b
		return err
	})
	return ret, err
}

func (s *Space) Delete(ctx context.Context, k string) (err error) {
	err = s.hook(ctx, "DELETE", k, func(ctx context.Context) error {
		return s.inner.Delete(ctx, k)
	})
	return err
}

func (s *Space) Get(ctx context.Context, k string) (ret *branches.Info, err error) {
	err = s.hook(ctx, "GET", k, func(ctx context.Context) error {
		b, err := s.inner.Get(ctx, k)
		ret = b
		return err
	})
	return ret, err
}

func (s *Space) Set(ctx context.Context, k string, md branches.Config) (err error) {
	err = s.hook(ctx, "SET", k, func(ctx context.Context) error {
		return s.inner.Set(ctx, k, md)
	})
	return err
}

func (s *Space) List(ctx context.Context, span branches.Span, limit int) (ret []string, err error) {
	err = s.hook(ctx, "LIST", "", func(ctx context.Context) error {
		var err error
		ret, err = s.inner.List(ctx, span, limit)
		return err
	})
	return ret, err
}

func (s *Space) Open(ctx context.Context, name string) (branches.Volume, error) {
	var vol branches.Volume
	if err := s.hook(ctx, "OPEN", name, func(ctx context.Context) error {
		var err error
		vol, err = s.inner.Open(ctx, name)
		return err
	}); err != nil {
		return nil, err
	}
	return vol, nil
}
