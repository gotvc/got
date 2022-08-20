package pkecell

import (
	"encoding/binary"
	"io"

	"github.com/cloudflare/circl/pke/kyber/kyber1024"
	"golang.org/x/crypto/ed25519"
)

type KEMScheme[Private, Public any] struct {
	Generate func(rng io.Reader) (Private, Public, error)

	Encapsulate func(pub Public, seed, ss *[32]byte, ct []byte)
	Decapsulate func(priv Private, ss *[32]byte, ct []byte)

	MarshalPublic func(Public) []byte
	ParsePublic   func([]byte) (Public, error)

	PublicKeySize  int
	CiphertextSize int
}

type SigScheme[Private, Public any] struct {
	Generate func(rng io.Reader) (Private, Public, error)
	Sign     func(out []byte, priv Private, msg []byte) []byte
	Verify   func(pub Public, msg []byte, sig []byte) bool

	PublicKeySize int
	SignatureSize int
}

type Scheme[KEMPriv, KEMPub, SigPriv, SigPub any] struct {
	KEM  KEMScheme[KEMPriv, KEMPub]
	Sign SigScheme[SigPriv, SigPub]
}

type PrivateKey[KEMPriv, SigPriv any] struct {
	KEM  KEMPriv
	Sign SigPriv
}

type PublicKey[KEMPub, SigPub any] struct {
	KEM  KEMPub
	Sign SigPub
}

func NewV1() Scheme[*kyber1024.PrivateKey, *kyber1024.PublicKey, ed25519.PrivateKey, ed25519.PublicKey] {
	return Scheme[*kyber1024.PrivateKey, *kyber1024.PublicKey, ed25519.PrivateKey, ed25519.PublicKey]{
		KEM:  NewKyber1024(),
		Sign: NewEd25519(),
	}
}

// Encrypt encrypts cell contents from priv for multiple parties.
func Encrypt[KEMPriv, KEMPub, SigPriv, SigPub any](scheme *Scheme[KEMPriv, KEMPub, SigPriv, SigPub], rng io.Reader, priv PrivateKey[KEMPriv, SigPriv], pubs []PublicKey[KEMPub, SigPub], out, pt []byte) ([]byte, error) {
	// generate random secret
	secret := [32]byte{}
	if _, err := io.ReadFull(rng, secret[:]); err != nil {
		return nil, err
	}
	// AEAD seal
	nonce := [8]byte{}
	ctext := aeadSeal(&secret, &nonce, ctext, ptext, nonce)

	// generate slots for all public keys
	var slots []byte
	for i := range pubs {
		slot := encryptSlot(scheme, priv.Sign, pubs[i].KEM, &secret, &secret)
		slots = append(slots, slot...)
	}
	out = appendVarint(out, uint64(len(slots)))
	out = append(out)
	out = appendVarint(out, uint64(len(ctext)))
	out = append(out, ctext)
	return out, nil
}

const AEADOverhead = 16

func Decrypt[KEMPriv, KEMPub, SigPriv, SigPub any](scheme Scheme[KEMPriv, KEMPub, SigPriv, SigPub], priv PrivateKey[KEMPriv, SigPriv], pubs []PublicKey[KEMPriv, SigPriv], out, ct []byte) ([]byte, error) {
	slotSize := 32 + scheme.Sign.SignatureSize + AEADOverhead
	panic(slotSize)
}

func encryptSlot[KEMPriv, KEMPub, SigPriv, SigPub any](scheme *Scheme[KEMPriv, KEMPub, SigPriv, SigPub], priv SigPriv, pub KEMPub, rng io.Reader, payload *[32]byte) ([]byte, error) {
	seed := [32]byte{}
	if _, err := io.ReadFull(rng, seed[:]); err != nil {
		return nil, err
	}
	// KEM encapsulate
	var sharedSecret [32]byte
	kemct := make([]byte, scheme.KEM.CiphertextSize)
	scheme.KEM.Encapsulate(pub, &seed, &sharedSecret, kemct)

	// sign the KEM shared secret
	sig := scheme.Sign.Sign(nil, priv, sharedSecret[:])
	var ptext []byte
	ptext = append(ptext, payload[:]...)
	ptext = append(ptext, sig...)

	nonce := [8]byte{}
	ctext := make([]byte, len(ptext)+AEADOverhead)
	aeadSeal(&sharedSecret, &nonce, ctext, ptext, nil)

	return append(keyct, ctext...), nil
}

func decryptSlot[KEMPriv, KEMPub, SigPriv, SigPub any](scheme Scheme[KEMPriv, KEMPub, SigPriv, SigPub], priv PrivateKey[KEMPriv, SigPriv], pubs PublicKey[KEMPub, SigPub], ctext []byte) (*[32]byte, error) {
	panic("")
}

func appendVarint(out []byte, x uint64) []byte {
	buf := [binary.MaxVarintLen64]byte{}
	binary.PutUvarint(buf[:], x)
	return append(out, buf[:]...)
}
