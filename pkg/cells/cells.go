package cells

import (
	"context"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cells"
	"github.com/brendoncarroll/go-state/cells/cryptocell"
)

type Cell = cells.Cell

func Apply(ctx context.Context, cell Cell, fn func([]byte) ([]byte, error)) error {
	return cells.Apply(ctx, cell, fn)
}

func GetBytes(ctx context.Context, cell Cell) ([]byte, error) {
	return cells.GetBytes(ctx, cell)
}

func NewMem() cells.Cell {
	return cells.NewMem(1 << 16)
}

func NewSecretBox(inner cells.Cell, secret []byte) Cell {
	return cryptocell.NewSecretBox(inner, secret)
}

func NewSigned(inner cells.Cell, pubKey p2p.PublicKey, privKey p2p.PrivateKey) Cell {
	panic("not implemented")
	// return p2pcryptocell.NewSigned(inner, "got/signed-cell", pubKey, privKey)
}
