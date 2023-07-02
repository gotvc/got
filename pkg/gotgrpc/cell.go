package gotgrpc

import (
	"context"

	"github.com/brendoncarroll/go-state/cells"
)

var _ cells.BytesCell = &Cell{}

type Cell struct {
	c   SpaceClient
	key string
}

func (c *Cell) Load(ctx context.Context, dst *[]byte) error {
	res, err := c.c.ReadCell(ctx, &ReadCellReq{Key: c.key})
	if err != nil {
		return err
	}
	*dst = append((*dst)[:0], res.Data...)
	return nil
}

func (c *Cell) CAS(ctx context.Context, actual *[]byte, prev, next []byte) (bool, error) {
	res, err := c.c.CASCell(ctx, &CASCellReq{
		Key:  c.key,
		Prev: prev[:],
		Next: next,
	})
	if err != nil {
		return false, err
	}
	cells.CopyBytes(actual, res.Current)
	return res.Swapped, nil
}

func (c *Cell) MaxSize() int {
	return MaxCellSize
}

func (c *Cell) Copy(dst *[]byte, src []byte) {
	cells.CopyBytes(dst, src)
}

func (c *Cell) Equals(a, b []byte) bool {
	return cells.EqualBytes(a, b)
}
