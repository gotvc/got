package branchintc

import (
	"context"

	"go.brendoncarroll.net/state/cells"
)

type cellHook = func(ctx context.Context, verb Verb, next func(ctx context.Context) error) error

var _ cells.BytesCell = &Cell{}

type Cell struct {
	inner cells.BytesCell
	hook  cellHook
	cells.BytesCellBase
}

func newCell(x cells.BytesCell, hook cellHook) *Cell {
	return &Cell{inner: x, hook: hook}
}

func (c *Cell) CAS(ctx context.Context, actual *[]byte, prev, next []byte) (swapped bool, err error) {
	err = c.hook(ctx, Verb_CASCell, func(ctx context.Context) error {
		var err error
		swapped, err = c.inner.CAS(ctx, actual, prev, next)
		return err
	})
	return swapped, err
}

func (c *Cell) Load(ctx context.Context, dst *[]byte) (err error) {
	err = c.hook(ctx, Verb_ReadCell, func(ctx context.Context) error {
		err := c.inner.Load(ctx, dst)
		return err
	})
	return err
}

func (c *Cell) MaxSize() int {
	return c.inner.MaxSize()
}
