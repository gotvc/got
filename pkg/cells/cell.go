package cells

import (
	"context"

	"github.com/brendoncarroll/go-p2p"
)

type Cell = p2p.Cell

func Apply(ctx context.Context, cell Cell, fn func([]byte) ([]byte, error)) error {
	return p2p.Apply(ctx, cell, fn)
}
