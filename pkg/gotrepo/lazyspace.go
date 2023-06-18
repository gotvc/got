package gotrepo

import (
	"context"
	"sync"

	"github.com/gotvc/got/pkg/branches"
)

var _ branches.Space = &lazySpace{}

type lazySpace struct {
	once     sync.Once
	err      error
	space    branches.Space
	newSpace func(ctx context.Context) (branches.Space, error)
}

func newLazySpace(fn func(ctx context.Context) (branches.Space, error)) *lazySpace {
	return &lazySpace{newSpace: fn}
}

func (ls *lazySpace) Create(ctx context.Context, name string, params branches.Metadata) (*branches.Branch, error) {
	space, err := ls.getSpace(ctx)
	if err != nil {
		return nil, err
	}
	return space.Create(ctx, name, params)
}

func (ls *lazySpace) Get(ctx context.Context, name string) (*branches.Branch, error) {
	space, err := ls.getSpace(ctx)
	if err != nil {
		return nil, err
	}
	return space.Get(ctx, name)
}

func (ls *lazySpace) Set(ctx context.Context, name string, md branches.Metadata) error {
	space, err := ls.getSpace(ctx)
	if err != nil {
		return err
	}
	return space.Set(ctx, name, md)
}

func (ls *lazySpace) Delete(ctx context.Context, name string) error {
	space, err := ls.getSpace(ctx)
	if err != nil {
		return err
	}
	return space.Delete(ctx, name)
}

func (ls *lazySpace) List(ctx context.Context, span branches.Span, limit int) ([]string, error) {
	space, err := ls.getSpace(ctx)
	if err != nil {
		return nil, err
	}
	return space.List(ctx, span, limit)
}

func (ls *lazySpace) getSpace(ctx context.Context) (branches.Space, error) {
	ls.once.Do(func() {
		ls.space, ls.err = ls.newSpace(ctx)
	})
	return ls.space, ls.err
}
