package pkecell

import (
	"io"

	"github.com/cloudflare/circl/kem/kyber/kyber1024"
	"github.com/gotvc/got/pkg/cells/pkecell/kem"
)

func NewKyber1024() kem.Scheme[*kyber1024.PrivateKey, *kyber1024.PublicKey] {
	type (
		Private = *kyber1024.PrivateKey
		Public  = *kyber1024.PublicKey
	)
	return kem.Scheme[Private, Public]{
		Generate: func(rng io.Reader) (Private, Public, error) {
			pub, priv, err := kyber1024.GenerateKeyPair(rng)
			return priv, pub, err
		},
		DerivePublic: func(x Private) Public {
			return x.Public().(Public)
		},
		Encapsulate: func(pub Public, seed *[32]byte, ss *[32]byte, ct []byte) error {
			pub.EncapsulateTo(ct, ss[:], seed[:])
			return nil
		},
		Decapsulate: func(priv Private, ss *[32]byte, ct []byte) error {
			priv.DecapsulateTo(ss[:], ct[:])
			return nil
		},
		MarshalPublic: func(x Public) []byte {
			buf := make([]byte, kyber1024.PublicKeySize)
			x.Pack(buf)
			return buf
		},
		ParsePublic: func(data []byte) (Public, error) {
			pub, err := kyber1024.Scheme().UnmarshalBinaryPublicKey(data)
			if err != nil {
				return nil, err
			}
			return pub.(Public), err
		},
		PublicKeySize:  kyber1024.PublicKeySize,
		CiphertextSize: kyber1024.CiphertextSize,
	}
}
