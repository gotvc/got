package sign

import (
	"io"

	"github.com/pkg/errors"
)

type Scheme[Private, Public any] struct {
	Generate     func(rng io.Reader) (Private, Public, error)
	DerivePublic func(Private) Public

	Sign   func(dst []byte, priv Private, msg []byte)
	Verify func(pub Public, msg []byte, sig []byte) bool

	MarshalPublic func(Public) []byte
	ParsePublic   func([]byte) (Public, error)

	PublicKeySize int
	SignatureSize int
}

// DualKey is a hybrid key made of 2 keys
type DualKey[A, B any] struct {
	A A
	B B
}

func NewDualSig[APriv, BPriv, APub, BPub any](a Scheme[APriv, APub], b Scheme[BPriv, BPub]) Scheme[DualKey[APriv, BPriv], DualKey[APub, BPub]] {
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
		PublicKeySize: a.PublicKeySize + b.PublicKeySize,
		SignatureSize: a.SignatureSize + b.SignatureSize,
	}
}
