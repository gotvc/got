package pkecell

import (
	"io"

	"github.com/pkg/errors"
	"golang.org/x/crypto/sha3"
)

// DualKey is a hybrid key made of 2 keys
type DualKey[A, B any] struct {
	A A
	B B
}

func NewDualKEM[APriv, BPriv, APub, BPub any](a KEMScheme[APriv, APub], b KEMScheme[BPriv, BPub]) KEMScheme[DualKey[APriv, BPriv], DualKey[APub, BPub]] {
	type (
		Private = DualKey[APriv, BPriv]
		Public  = DualKey[APub, BPub]
	)
	return KEMScheme[Private, Public]{
		Generate: func(rng io.Reader) (Private, Public, error) {
			privA, pubA, err := a.Generate(rng)
			if err != nil {
				return Private{}, Public{}, err
			}
			privB, pubB, err := b.Generate(rng)
			if err != nil {
				return Private{}, Public{}, err
			}
			return Private{A: privA, B: privB}, Public{A: pubA, B: pubB}, nil
		},
		Encapsulate: func(pub Public, seed, ss *[32]byte, ct []byte) {
			var seedA, seedB [32]byte
			sha3.ShakeSum256(seedA[:], append(seed[:], 0))
			sha3.ShakeSum256(seedB[:], append(seed[:], 255))
			var sharedA, sharedB [32]byte
			a.Encapsulate(pub.A, &seedA, &sharedA, ct[:a.CiphertextSize])
			b.Encapsulate(pub.B, &seedB, &sharedB, ct[a.CiphertextSize:])
			var sharedConcat [64]byte
			copy(sharedConcat[:32], sharedA[:])
			copy(sharedConcat[32:], sharedB[:])
			sha3.ShakeSum256(ss[:], sharedConcat[:])
		},
		Decapsulate: func(priv Private, ss *[32]byte, ct []byte) {
			var sharedA, sharedB [32]byte
			a.Decapsulate(priv.A, &sharedA, ct[:a.CiphertextSize])
			b.Decapsulate(priv.B, &sharedB, ct[:b.CiphertextSize])
			var sharedConcat [64]byte
			copy(sharedConcat[:32], sharedA[:])
			copy(sharedConcat[32:], sharedB[:])
			sha3.ShakeSum256(ss[:], sharedConcat[:])
		},
		MarshalPublic: func(x Public) (ret []byte) {
			ret = append(ret, a.MarshalPublic(x.A)...)
			ret = append(ret, b.MarshalPublic(x.B)...)
			return ret
		},
		ParsePublic: func(x []byte) (Public, error) {
			if len(x) != a.PublicKeySize+b.PublicKeySize {
				return Public{}, errors.Errorf("too short to be public key len=%d", len(x))
			}
			aPub, err := a.ParsePublic(x[:a.PublicKeySize])
			if err != nil {
				return Public{}, err
			}
			bPub, err := b.ParsePublic(x[a.PublicKeySize:])
			if err != nil {
				return Public{}, err
			}
			return Public{A: aPub, B: bPub}, nil
		},
	}
}
