package pkecell

import (
	"io"

	"github.com/cloudflare/circl/kem/kyber/kyber1024"
)

func NewKyber1024() KEMScheme[*kyber1024.PrivateKey, *kyber1024.PublicKey] {
	type (
		Private = kyber1024.PrivateKey
		Public  = kyber1024.PublicKey
	)
	return KEMScheme[*Private, *Public]{
		Generate: func(rng io.Reader) (*Private, *Public, error) {
			pub, priv, err := kyber1024.GenerateKeyPair(rng)
			return priv, pub, err
		},
		Encapsulate: func(pub *Public, seed *[32]byte, ss *[32]byte, ct []byte) {
			pub.EncapsulateTo(ct, ss[:], seed[:])
		},
		Decapsulate: func(priv *Private, ss *[32]byte, ct []byte) {
			priv.DecapsulateTo(ss[:], ct[:])
		},
		MarshalPublic: func(x *Public) []byte {
			return nil
		},
		ParsePublic: func(data []byte) (*Public, error) {
			return nil, nil
		},
	}
}
