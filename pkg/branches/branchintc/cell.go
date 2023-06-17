package branchintc

import (
	"context"

	"github.com/gotvc/got/pkg/cells"
)

type cellHook = func(verb Verb, next func() error) error

var _ cells.Cell = &Cell{}

type Cell struct {
	inner cells.Cell
	hook  cellHook
}

func newCell(x cells.Cell, hook cellHook) *Cell {
	return &Cell{inner: x, hook: hook}
}

func (c *Cell) CAS(ctx context.Context, actual, prev, next []byte) (swapped bool, n int, err error) {
	err = c.hook(Verb_CASCell, func() error {
		var err error
		swapped, n, err = c.inner.CAS(ctx, actual, prev, next)
		return err
	})
	return swapped, n, err
}

func (c *Cell) Read(ctx context.Context, buf []byte) (n int, err error) {
	err = c.hook(Verb_ReadCell, func() error {
		var err error
		n, err = c.inner.Read(ctx, buf)
		return err
	})
	return n, err
}

func (c *Cell) MaxSize() int {
	return c.inner.MaxSize()
}
