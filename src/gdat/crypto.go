package gdat

import (
	"context"
	"encoding/hex"
	"io"
	"math"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/stdctx/logctx"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/chacha20"

	"github.com/gotvc/got/src/internal/stores"
)

func Hash(x []byte) blobcache.CID {
	return stores.Hash(x)
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
type KeyFunc func(ptextHash blobcache.CID) DEK

// SaltedConvergent uses salt to generate convergent keys for each blob.
func SaltedConvergent(salt *[32]byte) KeyFunc {
	salt = cloneSalt(salt)
	return func(ptextHash blobcache.CID) DEK {
		dek := DEK{}
		DeriveKey(dek[:], salt, ptextHash[:])
		return dek
	}
}

// Convergent generates a DEK depending only on ptextHash
func Convergent(ptextHash blobcache.CID) DEK {
	return DEK(ptextHash)
}

const DEKSize = 32

type DEK [DEKSize]byte

func (dek DEK) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(dek[:])), nil
}

func (dek *DEK) UnmarshalText(data []byte) error {
	_, err := hex.Decode(dek[:], data)
	return err
}

func (*DEK) String() string {
	return "{ 32 byte DEK }"
}

func (a *Machine) postEncrypt(ctx context.Context, s stores.Writing, keyFunc KeyFunc, data []byte) (blobcache.CID, *DEK, error) {
	dek := keyFunc(Hash(data))
	ctext := a.acquire(s.MaxSize())
	defer a.release(ctext)
	n := cryptoXOR(dek, ctext, data)
	id, err := s.Post(ctx, ctext[:n])
	if err != nil {
		return blobcache.CID{}, nil, err
	}
	return id, &dek, nil
}

func getDecrypt(ctx context.Context, s stores.Reading, dek DEK, id blobcache.CID, buf []byte) (int, error) {
	n, err := s.Get(ctx, id, buf)
	if err != nil {
		return 0, err
	}
	data := buf[:n]
	if err := cadata.Check(Hash, id, data); err != nil {
		logctx.Errorf(ctx, "len(data)=%d HAVE: %v WANT: %v", len(data), id, Hash(data))
		return 0, err
	}
	cryptoXOR(dek, data, data)
	return n, nil
}

func cryptoXOR(key DEK, dst, src []byte) int {
	nonce := [chacha20.NonceSize]byte{}
	cipher, err := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if err != nil {
		panic(err)
	}
	cipher.XORKeyStream(dst, src)
	return len(src)
}

func cloneSalt(x *[32]byte) *[32]byte {
	y := *x
	return &y
}
