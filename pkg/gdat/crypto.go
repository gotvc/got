package gdat

import (
	"context"
	"crypto/rand"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/got/pkg/cadata"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/blake3"
)

type KeyFunc func(ptextHash cadata.ID) DEK

func SaltedConvergent(salt []byte) KeyFunc {
	return func(ptextHash cadata.ID) DEK {
		h := blake3.New(32, salt)
		h.Write(ptextHash[:])
		dek := DEK{}
		h.Sum(dek[:0])
		return dek
	}
}

func Convergent(ptextHash cadata.ID) DEK {
	return DEK(blake3.Sum256(ptextHash[:]))
}

func RandomKey(cadata.ID) DEK {
	dek := DEK{}
	if _, err := rand.Read(dek[:]); err != nil {
		panic(err)
	}
	return dek
}

type DEK [32]byte

func postEncrypt(ctx context.Context, s cadata.Poster, keyFunc KeyFunc, data []byte) (blobs.ID, *DEK, error) {
	id := blobs.Hash(data)
	dek := keyFunc(id)
	ctext := make([]byte, len(data))
	cryptoXOR(dek, ctext, data)
	id, err := s.Post(ctx, ctext)
	if err != nil {
		return blobs.ID{}, nil, err
	}
	return id, &dek, nil
}

func getDecrypt(ctx context.Context, s cadata.Getter, dek DEK, id blobs.ID, fn func([]byte) error) error {
	var ptext []byte
	if err := s.GetF(ctx, id, func(ctext []byte) error {
		ptext = make([]byte, len(ctext))
		cryptoXOR(dek, ptext, ctext)
		return nil
	}); err != nil {
		return err
	}
	return fn(ptext)
}

func cryptoXOR(key DEK, dst, src []byte) {
	nonce := [chacha20.NonceSize]byte{}
	cipher, err := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if err != nil {
		panic(err)
	}
	cipher.XORKeyStream(dst, src)
}
