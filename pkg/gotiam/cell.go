package gotiam

import (
	"context"

	"github.com/gotvc/got/pkg/cells"
)

var _ cells.Cell = &Cell{}

type Cell struct {
	inner cells.Cell
	check checkFn
}

func newCell(x cells.Cell, check checkFn) *Cell {
	return &Cell{inner: x, check: check}
}

func (c *Cell) CAS(ctx context.Context, actual, prev, next []byte) (bool, int, error) {
	if err := c.check(true, "CAS"); err != nil {
		return false, -1, err
	}
	return c.inner.CAS(ctx, actual, prev, next)
}

func (c *Cell) Read(ctx context.Context, buf []byte) (int, error) {
	if err := c.check(false, "READ"); err != nil {
		return -1, err
	}
	return c.inner.Read(ctx, buf)
}

func (c *Cell) MaxSize() int {
	return c.inner.MaxSize()
}
