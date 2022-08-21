package pkecell

import (
	"io"

	dilithium5 "github.com/cloudflare/circl/sign/dilithium/mode5"
	"github.com/gotvc/got/pkg/cells/pkecell/sign"
)

func NewDilithium5() sign.Scheme[*dilithium5.PrivateKey, *dilithium5.PublicKey] {
	type (
		Private = *dilithium5.PrivateKey
		Public  = *dilithium5.PublicKey
	)
	return sign.Scheme[Private, Public]{
		Generate: func(rng io.Reader) (Private, Public, error) {
			pub, priv, err := dilithium5.GenerateKey(rng)
			return priv, pub, err
		},
		DerivePublic: func(x Private) Public {
			return x.Public().(Public)
		},
		Sign: func(dst []byte, priv Private, msg []byte) {
			dilithium5.SignTo(priv, msg, dst)
		},
		Verify: func(pub Public, msg, sig []byte) bool {
			return dilithium5.Verify(pub, msg, sig)
		},
		MarshalPublic: func(x Public) []byte {
			var buf [dilithium5.PublicKeySize]byte
			x.Pack(&buf)
			return buf[:]
		},
		ParsePublic: func(x []byte) (Public, error) {
			var pub dilithium5.PublicKey
			if err := pub.UnmarshalBinary(x); err != nil {
				return nil, err
			}
			return &pub, nil
		},
		SignatureSize: dilithium5.SignatureSize,
		PublicKeySize: dilithium5.PublicKeySize,
	}
}
