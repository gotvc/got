package pkecell

import (
	"fmt"
	"io"

	"github.com/cloudflare/circl/sign/ed448"
	"github.com/gotvc/got/pkg/cells/pkecell/sign"
)

func NewEd448() sign.Scheme[ed448.PrivateKey, ed448.PublicKey] {
	type (
		Private = ed448.PrivateKey
		Public  = ed448.PublicKey
	)
	return sign.Scheme[Private, Public]{
		Generate: func(rng io.Reader) (Private, Public, error) {
			pub, priv, err := ed448.GenerateKey(rng)
			return priv, pub, err
		},
		Sign: func(dst []byte, priv Private, msg []byte) {
			if len(dst) < ed448.SignatureSize {
				panic(fmt.Sprintf("dst too short. HAVE: %d WANT: >=%d", len(dst), ed448.SignatureSize))
			}
			sig := ed448.Sign(priv, msg, "")
			copy(dst, sig)
		},
		Verify: func(pub Public, msg, sig []byte) bool {
			return ed448.Verify(pub, msg, sig, "")
		},
		MarshalPublic: func(x Public) []byte {
			data, err := x.MarshalBinary()
			if err != nil {
				panic(err)
			}
			return data
		},
		ParsePublic: func(x []byte) (Public, error) {
			pub, err := ed448.Scheme().UnmarshalBinaryPublicKey(x)
			if err != nil {
				return nil, err
			}
			return pub.(Public), err
		},
		PublicKeySize: ed448.PublicKeySize,
		SignatureSize: ed448.SignatureSize,
	}
}
