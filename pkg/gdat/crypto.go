package gdat

import (
	"context"
	"io"
	"math"

	"github.com/brendoncarroll/go-state/cadata"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/chacha20"
)

func Hash(x []byte) cadata.ID {
	return blake2b.Sum256(x)
}

// DeriveKey uses the blake2b XOF to fill out.
// The input to the XOF is additional and secret is used to key the XOF.
func DeriveKey(out []byte, secret *[32]byte, additional []byte) {
	if len(out) == 0 {
		return
	}
	outputLength := uint32(blake2b.OutputLengthUnknown)
	if len(out) < math.MaxUint32 {
		outputLength = uint32(len(out))
	}
	xof, err := blake2b.NewXOF(outputLength, secret[:])
	if err != nil {
		panic(err)
	}
	if _, err := xof.Write(additional); err != nil {
		panic(err)
	}
	if _, err := io.ReadFull(xof, out[:]); err != nil {
		panic(err)
	}
}

// DeriveStream returns a cryptographically secure psuedorandom stream
// derived from a high entropy secret, and arbitrary additional bytes.
func DeriveStream(secret *[32]byte, additional []byte) io.Reader {
	outputLength := uint32(blake2b.OutputLengthUnknown)
	xof, err := blake2b.NewXOF(outputLength, secret[:])
	if err != nil {
		panic(err)
	}
	if _, err := xof.Write(additional); err != nil {
		panic(err)
	}
	return xof
}

// KeyFunc produces a key for a given blob
type KeyFunc func(ptextHash cadata.ID) DEK

// SaltedConvergent uses salt to generate convergent keys for each blob.
func SaltedConvergent(salt *[32]byte) KeyFunc {
	salt = cloneSalt(salt)
	return func(ptextHash cadata.ID) DEK {
		dek := DEK{}
		DeriveKey(dek[:], salt, ptextHash[:])
		return dek
	}
}

// Convergent generates a DEK depending only on ptextHash
func Convergent(ptextHash cadata.ID) DEK {
	return DEK(ptextHash)
}

const DEKSize = 32

type DEK [DEKSize]byte

func (*DEK) String() string {
	return "{ 32 byte DEK }"
}

func postEncrypt(ctx context.Context, s cadata.Poster, keyFunc KeyFunc, data []byte) (cadata.ID, *DEK, error) {
	dek := keyFunc(Hash(data))
	ctext := make([]byte, len(data))
	cryptoXOR(dek, ctext, data)
	id, err := s.Post(ctx, ctext)
	if err != nil {
		return cadata.ID{}, nil, err
	}
	return id, &dek, nil
}

func getDecrypt(ctx context.Context, s cadata.Store, dek DEK, id cadata.ID, buf []byte) (int, error) {
	n, err := s.Get(ctx, id, buf)
	if err != nil {
		return 0, err
	}
	data := buf[:n]
	if err := cadata.Check(s.Hash, id, data); err != nil {
		return 0, err
	}
	cryptoXOR(dek, data, data)
	return n, nil
}

func cryptoXOR(key DEK, dst, src []byte) {
	nonce := [chacha20.NonceSize]byte{}
	cipher, err := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if err != nil {
		panic(err)
	}
	cipher.XORKeyStream(dst, src)
}

func cloneSalt(x *[32]byte) *[32]byte {
	y := *x
	return &y
}
