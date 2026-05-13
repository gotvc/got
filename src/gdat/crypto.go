package gdat

import (
	"context"
	"encoding/hex"
	"io"
	"math"

	"blobcache.io/blobcache/src/blobcache"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/chacha20"

	"github.com/gotvc/got/src/internal/stores"
)

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

func (m *Machine) postEncrypt(ctx context.Context, s stores.WO, data []byte) (blobcache.CID, *DEK, error) {
	h := m.khf(nil, data)
	dek := DEK(m.khf(&m.salt, h[:]))
	ctext := m.acquire(s.MaxSize())
	defer m.release(ctext)
	n := cryptoXOR(dek, ctext, data)
	id, err := s.Post(ctx, ctext[:n])
	if err != nil {
		return blobcache.CID{}, nil, err
	}
	return id, &dek, nil
}

func (m *Machine) getDecrypt(ctx context.Context, s stores.RO, dek DEK, id blobcache.CID, buf []byte) (int, error) {
	n, err := s.Get(ctx, id, buf)
	if err != nil {
		return 0, err
	}
	data := buf[:n]
	// We can assume blobcache checked the blob for us.
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
