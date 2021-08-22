package branches

import (
	"bytes"
	"context"
	"crypto/cipher"
	"encoding/base64"
	"fmt"

	"github.com/brendoncarroll/go-state/cells/cryptocell"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/pkg/errors"
	"golang.org/x/crypto/chacha20poly1305"
)

type CryptoSpace struct {
	inner  Space
	secret []byte
}

func NewCryptoSpace(inner Space, secret []byte) Space {
	return &CryptoSpace{
		inner:  inner,
		secret: append([]byte{}, secret...),
	}
}

func (r *CryptoSpace) Create(ctx context.Context, name string) (*Branch, error) {
	nameCtext := r.encryptName(name)
	return r.inner.Create(ctx, nameCtext)
}

func (r *CryptoSpace) Get(ctx context.Context, name string) (*Branch, error) {
	nameCtext := r.encryptName(name)
	branch, err := r.inner.Get(ctx, nameCtext)
	if err != nil {
		return nil, err
	}
	return &Branch{
		Volume: r.wrapVolume(name, branch.Volume),
	}, nil
}

func (r *CryptoSpace) Delete(ctx context.Context, name string) error {
	nameCtext := r.encryptName(name)
	return r.inner.Delete(ctx, nameCtext)
}

func (r *CryptoSpace) ForEach(ctx context.Context, fn func(string) error) error {
	return r.inner.ForEach(ctx, func(x string) error {
		y, err := r.decryptName(x)
		if err != nil {
			return err
		}
		return fn(y)
	})
}

func (r *CryptoSpace) getAEAD(secret []byte) cipher.AEAD {
	aead, err := chacha20poly1305.NewX(secret)
	if err != nil {
		panic(err)
	}
	return aead
}

func (r *CryptoSpace) encryptName(x string) string {
	var (
		secret [32]byte
		nonce  [24]byte
	)
	deriveKey(secret[:], r.secret, "got/realm/names")
	deriveKey(nonce[:], r.secret, "got/realm/name-nonces/"+x)
	ctext := r.getAEAD(secret[:]).Seal(nil, nonce[:], []byte(x), nil)
	return fmt.Sprintf("%s.%s", enc.EncodeToString(nonce[:]), enc.EncodeToString(ctext[:]))
}

func (r *CryptoSpace) decryptName(x string) (string, error) {
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

func (r *CryptoSpace) wrapVolume(name string, x Volume) Volume {
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
	gdat.DeriveKey(out, secret, []byte(purpose))
}

var enc = base64.URLEncoding
