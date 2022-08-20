package pkecell

import (
	"context"

	"github.com/gotvc/got/pkg/cells"
	"golang.org/x/crypto/chacha20poly1305"
)

type Cell[Private, Public any] struct {
	scheme KEMScheme[Private, Public]
	inner  cells.Cell
}

func New[Private, Public any](inner cells.Cell, scheme KEMScheme[Private, Public]) *Cell[Private, Public] {
	return &Cell[Private, Public]{inner: inner, scheme: scheme}
}

func (c *Cell[Private, Public]) CAS(ctx context.Context, actual, prev, next []byte) (bool, int, error) {
	panic("")
}

func (c *Cell[Private, Public]) Read(ctx context.Context, buf []byte) (int, error) {
	panic("")
}

func aeadSeal(secret *[32]byte, nonce *[8]byte, dst, ptext []byte, additional []byte) {
	aead, err := chacha20poly1305.New(secret[:])
	if err != nil {
		panic(err)
	}
	if len(dst) != len(ptext)-aead.Overhead() {
		panic(len(dst))
	}
	aead.Seal(dst[:0], nonce[:], ptext, additional)
}

func aeadOpen(secret *[32]byte, nonce *[8]byte, dst, ctext []byte, additional []byte) error {
	aead, err := chacha20poly1305.New(secret[:])
	if err != nil {
		return err
	}
	_, err = aead.Open(dst[:0], nonce[:], ctext, additional)
	return err
}

func xorBytes(dst []byte, a, b []byte) {
	if len(a) != len(b) || len(dst) != len(a) {
		panic(len(a))
	}
	for i := range dst {
		dst[i] = a[i] ^ b[i]
	}
}
