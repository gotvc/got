package cells

import (
	"context"
	"os"

	"github.com/brendoncarroll/go-p2p"
)

type Cell = p2p.Cell

func Apply(ctx context.Context, cell Cell, fn func([]byte) ([]byte, error)) error {
	return p2p.Apply(ctx, cell, fn)
}

var ErrNotExist = os.ErrNotExist

type CellSpace interface {
	ForEach(ctx context.Context, prefix string, fn func(k string) error) error
	Get(ctx context.Context, k string) (Cell, error)
}
