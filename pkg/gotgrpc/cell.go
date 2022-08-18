package gotgrpc

import (
	"context"
	"io"

	"github.com/gotvc/got/pkg/cells"
)

var _ cells.Cell = &Cell{}

type Cell struct {
	c   SpaceClient
	key string
}

func (c *Cell) Read(ctx context.Context, buf []byte) (int, error) {
	res, err := c.c.ReadCell(ctx, &ReadCellReq{Key: c.key})
	if err != nil {
		return 0, err
	}
	if len(buf) < len(res.Data) {
		return 0, io.ErrShortBuffer
	}
	return copy(buf, res.Data), nil
}

func (c *Cell) CAS(ctx context.Context, actual, prev, next []byte) (bool, int, error) {
	prevHash := Hash(prev)
	res, err := c.c.CASCell(ctx, &CASCellReq{
		Key:      c.key,
		PrevHash: prevHash[:],
		Next:     next,
	})
	if err != nil {
		return false, 0, err
	}
	if len(actual) < len(res.Current) {
		return false, 0, io.ErrShortBuffer
	}
	n := copy(actual, res.Current)
	return res.Swapped, n, nil
}

func (c *Cell) MaxSize() int {
	return MaxCellSize
}
