package pkecell

import (
	"github.com/cloudflare/circl/dh/x448"
	"github.com/cloudflare/circl/kem/kyber/kyber1024"
	"github.com/cloudflare/circl/sign/ed448"
	"github.com/gotvc/got/pkg/cells/pkecell/kem"
)

func NewSchemeV1() Scheme[kem.DualKey[x448.Key, *kyber1024.PrivateKey], kem.DualKey[x448.Key, *kyber1024.PublicKey], ed448.PrivateKey, ed448.PublicKey] {
	return Scheme[kem.DualKey[x448.Key, *kyber1024.PrivateKey], kem.DualKey[x448.Key, *kyber1024.PublicKey], ed448.PrivateKey, ed448.PublicKey]{
		KEM:  kem.NewDualKEM(NewX448(), NewKyber1024()),
		Sign: NewEd448(),
	}
}

type PrivateKeyV1 PrivateKey[kem.DualKey[x448.Key, *kyber1024.PrivateKey], ed448.PrivateKey]

type PublicKeyV1 PublicKey[kem.DualKey[x448.Key, *kyber1024.PublicKey], ed448.PublicKey]

type ParamsV1 struct {
	Private PrivateKeyV1
	Writers []PublicKeyV1
	Readers []PublicKeyV1
}

// func NewV1(inner cells.Cell, params ParamsV1) cells.Cell {
// 	return New(inner, NewSchemeV1(), params.Private, params.Writers, params.Readers)
// }
