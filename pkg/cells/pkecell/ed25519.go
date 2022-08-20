package pkecell

import (
	"crypto/ed25519"
	"io"
)

func NewEd25519() SigScheme[ed25519.PrivateKey, ed25519.PublicKey] {
	type (
		Private = ed25519.PrivateKey
		Public  = ed25519.PublicKey
	)
	return SigScheme[ed25519.PrivateKey, ed25519.PublicKey]{
		Generate: func(rng io.Reader) (Private, Public, error) {
			pub, priv, err := ed25519.GenerateKey(rng)
			return priv, pub, err
		},
		Sign: func(out []byte, priv Private, msg []byte) []byte {
			sig := ed25519.Sign(priv, msg)
			return append(out, sig...)
		},
		Verify: func(pub Public, msg, sig []byte) bool {
			return ed25519.Verify(pub, msg, sig)
		},
	}
}
