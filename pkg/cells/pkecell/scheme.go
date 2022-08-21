package pkecell

import (
	"io"

	"github.com/gotvc/got/pkg/cells/pkecell/kem"
	"github.com/gotvc/got/pkg/cells/pkecell/sign"
	"golang.org/x/crypto/chacha20poly1305"
)

type Scheme[KEMPriv, KEMPub, SigPriv, SigPub any] struct {
	KEM  kem.Scheme[KEMPriv, KEMPub]
	Sign sign.Scheme[SigPriv, SigPub]
}

func (s *Scheme[KEMPriv, KEMPub, SigPriv, SigPub]) Generate(rng io.Reader) (retPriv PrivateKey[KEMPriv, SigPriv], retPub PublicKey[KEMPub, SigPub], _ error) {
	kemPriv, kemPub, err := s.KEM.Generate(rng)
	if err != nil {
		return retPriv, retPub, err
	}
	signPriv, signPub, err := s.Sign.Generate(rng)
	if err != nil {
		return retPriv, retPub, err
	}
	retPriv = PrivateKey[KEMPriv, SigPriv]{KEM: kemPriv, Sign: signPriv}
	retPub = PublicKey[KEMPub, SigPub]{KEM: kemPub, Sign: signPub}
	return retPriv, retPub, nil
}

func (s *Scheme[KEMPriv, KEMPub, SigPriv, SigPub]) DerivePublic(priv PrivateKey[KEMPriv, SigPriv]) PublicKey[KEMPub, SigPub] {
	return PublicKey[KEMPub, SigPub]{
		KEM:  s.KEM.DerivePublic(priv.KEM),
		Sign: s.Sign.DerivePublic(priv.Sign),
	}
}

func (s *Scheme[KEMPriv, KEMPub, SigPriv, SigPub]) MarshalPublic(pub PublicKey[KEMPub, SigPub]) []byte {
	data := s.KEM.MarshalPublic(pub.KEM)
	data = append(data, s.Sign.MarshalPublic(pub.Sign)...)
	return data
}

func (s *Scheme[KEMPriv, KEMPub, SigPriv, SigPub]) ParsePublic(x []byte) (ret PublicKey[KEMPub, SigPub], _ error) {
	kemPub, err := s.KEM.ParsePublic(x[:s.KEM.PublicKeySize])
	if err != nil {
		return ret, err
	}
	sigPub, err := s.Sign.ParsePublic(x[s.KEM.PublicKeySize:])
	if err != nil {
		return ret, err
	}
	return PublicKey[KEMPub, SigPub]{KEM: kemPub, Sign: sigPub}, nil
}

func (s *Scheme[KEMPriv, KEMPub, SigPriv, SigPub]) Encrypt(dst []byte, private SigPriv, pubs []KEMPub, ptext []byte) {
	if len(dst) < len(ptext)+s.Overhead(len(pubs)) {
		panic("dst is too short")
	}
}

func (s *Scheme[KEMPriv, KEMPub, SigPriv, SigPub]) Decrypt(dst []byte, private KEMPriv, pubs []SigPub, ctext []byte) error {
	if len(dst) < len(ctext)-s.Overhead(len(pubs)) {
		panic("dst is too short")
	}
	return nil
}

func (s *Scheme[KEMPriv, KEMPub, SigPriv, SigPub]) Overhead(numParties int) int {
	const AEADOverhead = chacha20poly1305.Overhead
	const AEADKeySize = 32
	slotSize := s.KEM.CiphertextSize + (s.Sign.SignatureSize + AEADKeySize) + AEADOverhead
	return AEADOverhead + numParties*slotSize
}

type PrivateKey[KEMPriv, SigPriv any] struct {
	KEM  KEMPriv
	Sign SigPriv
}

type PublicKey[KEMPub, SigPub any] struct {
	KEM  KEMPub
	Sign SigPub
}

// // Encrypt encrypts cell contents from priv for multiple parties.
// func Encrypt[KEMPriv, KEMPub, SigPriv, SigPub any](scheme *Scheme[KEMPriv, KEMPub, SigPriv, SigPub], rng io.Reader, priv PrivateKey[KEMPriv, SigPriv], pubs []PublicKey[KEMPub, SigPub], out, pt []byte) ([]byte, error) {
// 	// generate random secret
// 	secret := [32]byte{}
// 	if _, err := io.ReadFull(rng, secret[:]); err != nil {
// 		return nil, err
// 	}
// 	// AEAD seal
// 	nonce := [8]byte{}
// 	ctext := aeadSeal(&secret, &nonce, ctext, ptext, nonce)

// 	// generate slots for all public keys
// 	var slots []byte
// 	for i := range pubs {
// 		slot := encryptSlot(scheme, priv.Sign, pubs[i].KEM, &secret, &secret)
// 		slots = append(slots, slot...)
// 	}
// 	out = appendVarint(out, uint64(len(slots)))
// 	out = append(out)
// 	out = appendVarint(out, uint64(len(ctext)))
// 	out = append(out, ctext)
// 	return out, nil
// }

// const AEADOverhead = 16

// func Decrypt[KEMPriv, KEMPub, SigPriv, SigPub any](scheme Scheme[KEMPriv, KEMPub, SigPriv, SigPub], priv PrivateKey[KEMPriv, SigPriv], pubs []PublicKey[KEMPriv, SigPriv], out, ct []byte) ([]byte, error) {
// 	slotSize := 32 + scheme.Sign.SignatureSize + AEADOverhead
// 	panic(slotSize)
// }

// func encryptSlot[KEMPriv, KEMPub, SigPriv, SigPub any](scheme *Scheme[KEMPriv, KEMPub, SigPriv, SigPub], priv SigPriv, pub KEMPub, rng io.Reader, payload *[32]byte) ([]byte, error) {
// 	seed := [32]byte{}
// 	if _, err := io.ReadFull(rng, seed[:]); err != nil {
// 		return nil, err
// 	}
// 	// KEM encapsulate
// 	var sharedSecret [32]byte
// 	kemct := make([]byte, scheme.KEM.CiphertextSize)
// 	scheme.KEM.Encapsulate(pub, &seed, &sharedSecret, kemct)

// 	// sign the KEM shared secret
// 	sig := scheme.Sign.Sign(nil, priv, sharedSecret[:])
// 	var ptext []byte
// 	ptext = append(ptext, payload[:]...)
// 	ptext = append(ptext, sig...)

// 	nonce := [8]byte{}
// 	ctext := make([]byte, len(ptext)+AEADOverhead)
// 	aeadSeal(&sharedSecret, &nonce, ctext, ptext, nil)

// 	return append(kemct, ctext...), nil
// }

// func decryptSlot[KEMPriv, KEMPub, SigPriv, SigPub any](scheme Scheme[KEMPriv, KEMPub, SigPriv, SigPub], priv PrivateKey[KEMPriv, SigPriv], pubs PublicKey[KEMPub, SigPub], ctext []byte) (*[32]byte, error) {
// 	panic("")
// }
