package pkecell

import (
	"errors"
	"fmt"
	"io"

	"github.com/cloudflare/circl/dh/x448"
	"github.com/gotvc/got/pkg/cells/pkecell/kem"
	"golang.org/x/crypto/sha3"
)

func NewX448() kem.Scheme[x448.Key, x448.Key] {
	type (
		Private = x448.Key
		Public  = x448.Key
	)
	return kem.Scheme[Private, Public]{
		Generate: func(rng io.Reader) (Private, Public, error) {
			var pub, priv x448.Key
			if _, err := io.ReadFull(rng, priv[:]); err != nil {
				return Private{}, Public{}, err
			}
			x448.KeyGen(&pub, &priv)
			return priv, pub, nil
		},
		Encapsulate: func(pub Public, seed, secret *[32]byte, ct []byte) error {
			if len(ct) != x448.Size {
				panic(fmt.Sprintf("ct is wrong length HAVE: %d WANT: %d", len(ct), x448.Size))
			}
			var ePub, ePriv x448.Key
			sha3.ShakeSum256(ePriv[:], seed[:])
			x448.KeyGen(&ePub, &ePriv)
			var shared x448.Key
			if !x448.Shared(&shared, &ePriv, &pub) {
				return errors.New("error calculating shared key")
			}
			copy(ct, ePub[:])
			sha3.ShakeSum256(secret[:], shared[:])
			return nil
		},
		Decapsulate: func(priv Private, secret *[32]byte, ctext []byte) error {
			pub := (*[x448.Size]byte)(ctext)
			var shared x448.Key
			x448.Shared(&shared, &priv, (*x448.Key)(pub))
			sha3.ShakeSum256(secret[:], shared[:])
			return nil
		},
		MarshalPublic: func(pub Public) []byte {
			return pub[:]
		},
		ParsePublic: func(x []byte) (Public, error) {
			if len(x) != x448.Size {
				return Public{}, errors.New("wrong length for public key")
			}
			pub := Public{}
			copy(pub[:], x)
			return pub, nil
		},
		CiphertextSize: x448.Size,
		PublicKeySize:  x448.Size,
	}
}
