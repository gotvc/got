package gotrepo

import (
	"context"
	"sync"

	"github.com/gotvc/got/src/marks"
)

var _ marks.Space = &lazySpace{}

type lazySpace struct {
	once     sync.Once
	err      error
	space    marks.Space
	newSpace func(ctx context.Context) (marks.Space, error)
}

func newLazySpace(fn func(ctx context.Context) (marks.Space, error)) *lazySpace {
	return &lazySpace{newSpace: fn}
}

func (ls *lazySpace) Create(ctx context.Context, name string, params marks.Params) (*marks.Info, error) {
	space, err := ls.getSpace(ctx)
	if err != nil {
		return nil, err
	}
	return space.Create(ctx, name, params)
}

func (ls *lazySpace) Inspect(ctx context.Context, name string) (*marks.Info, error) {
	space, err := ls.getSpace(ctx)
	if err != nil {
		return nil, err
	}
	return space.Inspect(ctx, name)
}

func (ls *lazySpace) Set(ctx context.Context, name string, md marks.Params) error {
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

func (ls *lazySpace) List(ctx context.Context, span marks.Span, limit int) ([]string, error) {
	space, err := ls.getSpace(ctx)
	if err != nil {
		return nil, err
	}
	return space.List(ctx, span, limit)
}

func (ls *lazySpace) Open(ctx context.Context, name string) (*marks.Mark, error) {
	space, err := ls.getSpace(ctx)
	if err != nil {
		return nil, err
	}
	return space.Open(ctx, name)
}

func (ls *lazySpace) getSpace(ctx context.Context) (marks.Space, error) {
	ls.once.Do(func() {
		ls.space, ls.err = ls.newSpace(ctx)
	})
	return ls.space, ls.err
}
