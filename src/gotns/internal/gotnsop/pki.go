package gotnsop

import (
	"fmt"
	"maps"
	"strings"

	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/kem/mlkem/mlkem1024"
	"github.com/cloudflare/circl/sign"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/cloudflare/circl/sign/mldsa/mldsa87"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/sbe"
	"go.inet256.org/inet256/src/inet256"
)

type Secret [32]byte

func (s Secret) DeriveKEM() (kem.PublicKey, kem.PrivateKey) {
	var seed [64]byte
	gdat.DeriveKey(seed[:], (*[32]byte)(&s), []byte(KEM_MLKEM1024))
	return mlkem1024.Scheme().DeriveKeyPair(seed[:])
}

func (s Secret) DeriveSym() [32]byte {
	var ret [32]byte
	gdat.DeriveKey(ret[:], (*[32]byte)(&s), []byte("chacha20"))
	return ret
}

func (s Secret) Ratchet(n int) Secret {
	for range n {
		s = Secret(gdat.Hash(s[:]))
	}
	return s
}

// MarshalKEMPublicKey marshals a KEM public key with a scheme tag.
func MarshalKEMPublicKey(out []byte, tag string, kem kem.PublicKey) []byte {
	out = sbe.AppendLP16(out, []byte(tag))
	kemData, err := kem.MarshalBinary()
	if err != nil {
		panic(err)
	}
	out = append(out, kemData...)
	return out
}

// ParseKEMPublicKey unmarshals a KEM public key with a scheme tag.
func ParseKEMPublicKey(data []byte) (kem.PublicKey, error) {
	tag, rest, err := sbe.ReadLP16(data)
	if err != nil {
		return nil, err
	}
	scheme := getKEMScheme(string(tag))
	if scheme == nil {
		return nil, fmt.Errorf("unknown kem scheme: %s", string(tag))
	}
	data = rest
	pubKey, err := scheme.UnmarshalBinaryPublicKey(data)
	if err != nil {
		return nil, err
	}
	return pubKey, nil
}

func MarshalKEMPrivateKey(out []byte, tag string, privKey kem.PrivateKey) []byte {
	tag = tag + ".private"
	out = sbe.AppendLP16(out, []byte(tag))
	kemData, err := privKey.MarshalBinary()
	if err != nil {
		panic(err)
	}
	out = append(out, kemData...)
	return out
}

func ParseKEMPrivateKey(data []byte) (kem.PrivateKey, error) {
	tag, rest, err := sbe.ReadLP16(data)
	if err != nil {
		return nil, err
	}
	schemeName, ok := strings.CutSuffix(string(tag), ".private")
	if !ok {
		return nil, fmt.Errorf("kem private key tag does not end with .private")
	}
	data = rest
	scheme := getKEMScheme(schemeName)
	if scheme == nil {
		return nil, fmt.Errorf("unknown kem scheme: %s", tag)
	}
	privKey, err := scheme.UnmarshalBinaryPrivateKey(data)
	if err != nil {
		return nil, err
	}
	return privKey, nil
}

const (
	KEM_MLKEM1024 = "mlkem1024"
)

func getKEMScheme(tag string) kem.Scheme {
	switch tag {
	case KEM_MLKEM1024:
		return mlkem1024.Scheme()
	default:
		return nil
	}
}

const (
	SIGN_ED25519 = inet256.SignAlgo_Ed25519
	SIGN_MLDSA87 = "mldsa87"
)

func DeriveSign(seed [32]byte) (sign.PublicKey, sign.PrivateKey) {
	return mldsa87.Scheme().DeriveKey(seed[:])
}

var pki = inet256.PKI{
	Default: SIGN_ED25519,
	Schemes: map[string]sign.Scheme{
		SIGN_ED25519: ed25519.Scheme(),
		SIGN_MLDSA87: mldsa87.Scheme(),
	},
}

func PKI() inet256.PKI {
	return inet256.PKI{
		Default: pki.Default,
		Schemes: maps.Clone(pki.Schemes),
	}
}
