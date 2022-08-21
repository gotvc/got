package pkecell

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gotvc/got/pkg/cells"
	"golang.org/x/crypto/chacha20poly1305"
)

type Cell[KEMPriv, KEMPub, SigPriv, SigPub any] struct {
	scheme  Scheme[KEMPriv, KEMPub, SigPriv, SigPub]
	private PrivateKey[KEMPriv, SigPriv]
	writers []PublicKey[KEMPub, SigPub]
	readers []PublicKey[KEMPub, SigPub]
	inner   cells.Cell
}

func New[KEMPriv, KEMPub, SigPriv, SigPub any](inner cells.Cell, scheme Scheme[KEMPriv, KEMPub, SigPriv, SigPub], private PrivateKey[KEMPriv, SigPriv], writers, readers []PublicKey[KEMPub, SigPub]) *Cell[KEMPriv, KEMPub, SigPriv, SigPub] {
	return &Cell[KEMPriv, KEMPub, SigPriv, SigPub]{
		inner:   inner,
		scheme:  scheme,
		private: private,
		writers: writers,
		readers: readers,
	}
}

func (c *Cell[KEMPriv, KEMPub, SigPriv, SigPub]) CAS(ctx context.Context, actual, prev, next []byte) (bool, int, error) {
	// check if the public key for private is in writers.
	panic("")
}

func (c *Cell[KEMPriv, KEMPub, SigPriv, SigPub]) Read(ctx context.Context, buf []byte) (int, error) {
	panic("")
}

func (c *Cell[KEMPriv, KEMPub, SigPriv, SigPub]) MaxSize() int {
	return c.inner.MaxSize() - c.scheme.Overhead(len(c.readers))
}

type Message struct {
	Slots []byte
	Main  []byte
}

func ParseMessage(x []byte) (*Message, error) {
	l, n := binary.Uvarint(x)
	if n <= 0 {
		return nil, errors.New("error parsing varint")
	}
	start := n
	end := start + int(l)
	if end > len(x) {
		return nil, fmt.Errorf("varint points out of bounds")
	}
	slots := x[start:end]
	l2, n2 := binary.Uvarint(x[end:])
	if n <= 0 {
		return nil, errors.New("error parsing varint")
	}
	start = end + n2
	end = start + int(l2)
	if start >= len(x) || end > len(x) {
		return nil, fmt.Errorf("varint points out of bounds")
	}
	main := x[start:end]
	return &Message{
		Slots: slots,
		Main:  main,
	}, nil
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

func appendLP(out []byte, x []byte) []byte {
	out = appendVarint(out, uint64(len(x)))
	out = append(out, x...)
	return out
}

func appendVarint(out []byte, x uint64) []byte {
	buf := [binary.MaxVarintLen64]byte{}
	binary.PutUvarint(buf[:], x)
	return append(out, buf[:]...)
}
