package kem

import (
	"io"

	"github.com/pkg/errors"
	"golang.org/x/crypto/sha3"
)

const (
	SharedSecretSize = 32
	SeedSize         = 32
)

type Scheme[Private, Public any] struct {
	// Generate creates a new private/public key pair using entropy from rng.
	Generate func(rng io.Reader) (Private, Public, error)
	// DerivePublic returns the public key corresponding to the private key
	DerivePublic func(Private) Public

	Encapsulate func(pub Public, seed *[SeedSize]byte, ss *[SharedSecretSize]byte, ct []byte) error
	Decapsulate func(priv Private, ss *[SharedSecretSize]byte, ct []byte) error

	MarshalPublic func(Public) []byte
	ParsePublic   func([]byte) (Public, error)

	PublicKeySize  int
	CiphertextSize int
}

// DualKey is a hybrid key made of 2 keys
type DualKey[A, B any] struct {
	A A
	B B
}

func NewDualKEM[APriv, BPriv, APub, BPub any](a Scheme[APriv, APub], b Scheme[BPriv, BPub]) Scheme[DualKey[APriv, BPriv], DualKey[APub, BPub]] {
	return Scheme[DualKey[APriv, BPriv], DualKey[APub, BPub]]{
		Generate: func(rng io.Reader) (DualKey[APriv, BPriv], DualKey[APub, BPub], error) {
			privA, pubA, err := a.Generate(rng)
			if err != nil {
				return DualKey[APriv, BPriv]{}, DualKey[APub, BPub]{}, err
			}
			privB, pubB, err := b.Generate(rng)
			if err != nil {
				return DualKey[APriv, BPriv]{}, DualKey[APub, BPub]{}, err
			}
			return DualKey[APriv, BPriv]{A: privA, B: privB}, DualKey[APub, BPub]{A: pubA, B: pubB}, nil
		},
		Encapsulate: func(pub DualKey[APub, BPub], seed, ss *[32]byte, ct []byte) error {
			var seedA, seedB [SeedSize]byte
			sha3.ShakeSum256(seedA[:], append(seed[:], 0))
			sha3.ShakeSum256(seedB[:], append(seed[:], 255))

			var sharedConcat [64]byte
			sharedA := (*[SharedSecretSize]byte)(sharedConcat[:SharedSecretSize])
			sharedB := (*[SharedSecretSize]byte)(sharedConcat[SharedSecretSize:])

			a.Encapsulate(pub.A, &seedA, sharedA, ct[:a.CiphertextSize])
			b.Encapsulate(pub.B, &seedB, sharedB, ct[a.CiphertextSize:])

			sha3.ShakeSum256(ss[:], sharedConcat[:])
			return nil
		},
		Decapsulate: func(priv DualKey[APriv, BPriv], ss *[32]byte, ct []byte) error {
			var sharedConcat [2 * SharedSecretSize]byte
			sharedA := (*[SharedSecretSize]byte)(sharedConcat[:SharedSecretSize])
			sharedB := (*[SharedSecretSize]byte)(sharedConcat[SharedSecretSize:])

			a.Decapsulate(priv.A, sharedA, ct[:a.CiphertextSize])
			b.Decapsulate(priv.B, sharedB, ct[a.CiphertextSize:])

			sha3.ShakeSum256(ss[:], sharedConcat[:])
			return nil
		},
		MarshalPublic: func(x DualKey[APub, BPub]) (ret []byte) {
			ret = append(ret, a.MarshalPublic(x.A)...)
			ret = append(ret, b.MarshalPublic(x.B)...)
			return ret
		},
		ParsePublic: func(x []byte) (DualKey[APub, BPub], error) {
			if len(x) != a.PublicKeySize+b.PublicKeySize {
				return DualKey[APub, BPub]{}, errors.Errorf("too short to be public key len=%d", len(x))
			}
			aPub, err := a.ParsePublic(x[:a.PublicKeySize])
			if err != nil {
				return DualKey[APub, BPub]{}, err
			}
			bPub, err := b.ParsePublic(x[a.PublicKeySize:])
			if err != nil {
				return DualKey[APub, BPub]{}, err
			}
			return DualKey[APub, BPub]{A: aPub, B: bPub}, nil
		},
		CiphertextSize: a.CiphertextSize + b.CiphertextSize,
		PublicKeySize:  a.PublicKeySize + b.PublicKeySize,
	}
}
