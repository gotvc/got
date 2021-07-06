package branches

import (
	"bytes"
	"context"
	"crypto/cipher"
	"encoding/base64"
	"fmt"

	"github.com/brendoncarroll/go-state/cells/cryptocell"
	"github.com/pkg/errors"
	"golang.org/x/crypto/chacha20poly1305"
	"lukechampine.com/blake3"
)

type CryptoRealm struct {
	inner  Realm
	secret []byte
}

func NewCryptoRealm(inner Realm, secret []byte) Realm {
	return &CryptoRealm{
		inner:  inner,
		secret: append([]byte{}, secret...),
	}
}

func (r *CryptoRealm) Create(ctx context.Context, name string) error {
	nameCtext := r.encryptName(name)
	return r.inner.Create(ctx, nameCtext)
}

func (r *CryptoRealm) Get(ctx context.Context, name string) (*Branch, error) {
	nameCtext := r.encryptName(name)
	branch, err := r.inner.Get(ctx, nameCtext)
	if err != nil {
		return nil, err
	}
	return &Branch{
		Volume: r.wrapVolume(name, branch.Volume),
	}, nil
}

func (r *CryptoRealm) Delete(ctx context.Context, name string) error {
	nameCtext := r.encryptName(name)
	return r.inner.Delete(ctx, nameCtext)
}

func (r *CryptoRealm) ForEach(ctx context.Context, fn func(string) error) error {
	return r.inner.ForEach(ctx, func(x string) error {
		y, err := r.decryptName(x)
		if err != nil {
			return err
		}
		return fn(y)
	})
}

func (r *CryptoRealm) getAEAD(secret []byte) cipher.AEAD {
	aead, err := chacha20poly1305.NewX(secret)
	if err != nil {
		panic(err)
	}
	return aead
}

func (r *CryptoRealm) encryptName(x string) string {
	var (
		secret [32]byte
		nonce  [24]byte
	)
	deriveKey(secret[:], r.secret, "got/realm/names")
	deriveKey(nonce[:], r.secret, "got/realm/name-nonces/"+x)
	ctext := r.getAEAD(secret[:]).Seal(nil, nonce[:], []byte(x), nil)
	return fmt.Sprintf("%s.%s", enc.EncodeToString(nonce[:]), enc.EncodeToString(ctext[:]))
}

func (r *CryptoRealm) decryptName(x string) (string, error) {
	parts := bytes.SplitN([]byte(x), []byte{'.'}, 2)
	if len(parts) < 2 {
		return "", errors.Errorf("missing nonce")
	}
	var nonce [24]byte
	if _, err := enc.Decode(nonce[:], parts[0]); err != nil {
		return "", err
	}
	ctext := make([]byte, enc.DecodedLen(len(parts[1])))
	_, err := enc.Decode(ctext, parts[1])
	if err != nil {
		return "", err
	}
	var secret [32]byte
	deriveKey(secret[:], r.secret, "got/realm/names-secret")

	ptext, err := r.getAEAD(secret[:]).Open(nil, nonce[:], ctext, nil)
	if err != nil {
		return "", err
	}
	return string(ptext), nil
}

func (r *CryptoRealm) wrapVolume(name string, x Volume) Volume {
	var secret [32]byte
	deriveKey(secret[:], r.secret, "got/realm/cells/"+name)
	yCell := cryptocell.NewChaCha20Poly1305(x.Cell, secret[:])
	return Volume{
		Cell:     yCell,
		FSStore:  x.FSStore,
		VCStore:  x.VCStore,
		RawStore: x.RawStore,
	}
}

func deriveKey(out []byte, secret []byte, purpose string) {
	blake3.DeriveKey(out, purpose, secret)
}

var enc = base64.URLEncoding
