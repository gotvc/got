package cells

import (
	"context"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/c/cryptocell"
)

type Cell = p2p.Cell

func Apply(ctx context.Context, cell Cell, fn func([]byte) ([]byte, error)) error {
	return p2p.Apply(ctx, cell, fn)
}

type Signed = cryptocell.Signed

type SecretBoxCell = cryptocell.SecretBoxCell

func NewSecretBox(inner Cell, secret []byte) Cell {
	return cryptocell.NewSecretBox(inner, secret)
}

func NewSigned(inner Cell, purpose string, publicKey p2p.PublicKey, privateKey p2p.PrivateKey) Cell {
	return cryptocell.NewSigned(inner, purpose, publicKey, privateKey)
}
