package cells

import (
	"context"

	"go.brendoncarroll.net/state/cells"
	"go.brendoncarroll.net/state/cells/aeadcell"
	"golang.org/x/crypto/chacha20poly1305"
)

type (
	ErrTooLarge = cells.ErrTooLarge
)

type Cell = cells.BytesCell

func Apply(ctx context.Context, cell Cell, maxRetries int, fn func([]byte) ([]byte, error)) error {
	return cells.Apply[[]byte](ctx, cell, maxRetries, fn)
}

func GetBytes(ctx context.Context, cell Cell) ([]byte, error) {
	return cells.Load[[]byte](ctx, cell)
}

func NewMem() Cell {
	return cells.NewMemBytes(1 << 16)
}

func NewEncrypted(inner Cell, secret *[32]byte) Cell {
	aead, err := chacha20poly1305.NewX(secret[:])
	if err != nil {
		panic(err)
	}
	return aeadcell.New(inner, aead)
}
