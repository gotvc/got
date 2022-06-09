package branches

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/brendoncarroll/go-state/cells/cryptocell"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/exp/slices"
)

const (
	purposeBranchNames = "got/space/names"
	paddingBlockSize   = 16
)

type CryptoSpace struct {
	inner  Space
	secret *[32]byte
	log    *logrus.Logger
}

func NewCryptoSpace(inner Space, secret *[32]byte) Space {
	return &CryptoSpace{
		secret: secret,
		inner:  inner,
		log:    logrus.StandardLogger(),
	}
}

func (r *CryptoSpace) Create(ctx context.Context, name string, params Params) (*Branch, error) {
	nameCtext := r.encryptName(name)
	paramsCtext := r.encryptParams(params)
	return r.inner.Create(ctx, nameCtext, paramsCtext)
}

func (r *CryptoSpace) Get(ctx context.Context, name string) (*Branch, error) {
	nameCtext := r.encryptName(name)
	branch, err := r.inner.Get(ctx, nameCtext)
	if err != nil {
		return nil, err
	}
	salt, err := r.decryptSalt(branch.Salt)
	if err != nil {
		return nil, err
	}
	return &Branch{
		Volume: r.wrapVolume(name, branch.Volume),
		Salt:   salt,
	}, nil
}

func (r *CryptoSpace) Delete(ctx context.Context, name string) error {
	nameCtext := r.encryptName(name)
	return r.inner.Delete(ctx, nameCtext)
}

func (r *CryptoSpace) List(ctx context.Context, span Span, limit int) (ret []string, _ error) {
	err := ForEach(ctx, r.inner, TotalSpan(), func(x string) error {
		y, err := r.decryptName(x)
		if err != nil {
			r.handleDecryptFailure(x, err)
			return nil
		}
		if !span.Contains(y) {
			return nil
		}
		ret = append(ret, y)
		slices.Sort(ret)
		if limit > 0 && len(ret) >= limit {
			ret = ret[:limit]
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ret, nil
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
	deriveKey(secret[:], r.secret, purposeBranchNames)
	deriveKey(nonce[:], r.secret, "got/space/name-nonces/"+x)
	ptext := padBytes([]byte(x), paddingBlockSize)
	ctext := r.getAEAD(secret[:]).Seal(nil, nonce[:], ptext, nil)
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
	n, err := enc.Decode(ctext, parts[1])
	if err != nil {
		return "", err
	}
	ctext = ctext[:n]
	var secret [32]byte
	deriveKey(secret[:], r.secret, purposeBranchNames)
	ptext, err := r.getAEAD(secret[:]).Open(nil, nonce[:], ctext, nil)
	if err != nil {
		return "", err
	}
	name, err := unpadBytes(ptext, paddingBlockSize)
	if err != nil {
		return "", err
	}
	return string(name), nil
}

func (r *CryptoSpace) encryptParams(x Params) Params {
	return Params{
		Salt: r.encryptSalt(x.Salt),
	}
}

func (r *CryptoSpace) encryptSalt(x []byte) []byte {
	var secret [32]byte
	deriveKey(secret[:], r.secret, "got/space/branch-params")
	var nonce [24]byte
	readRandom(nonce[:])
	saltCtext := r.getAEAD(secret[:]).Seal(nil, nonce[:], x, nil)
	return append(nonce[:], saltCtext...)
}

func (r *CryptoSpace) decryptSalt(x []byte) ([]byte, error) {
	var secret [32]byte
	deriveKey(secret[:], r.secret, "got/space/branch-params")
	if len(x) < 24 {
		return nil, errors.Errorf("salt ctext not long enough to contain nonce len=%d", len(x))
	}
	nonce := x[:24]
	ctext := x[24:]
	return r.getAEAD(secret[:]).Open(nil, nonce, ctext, nil)
}

func (r *CryptoSpace) wrapVolume(name string, x Volume) Volume {
	var secret [32]byte
	deriveKey(secret[:], r.secret, "got/space/cells/"+name)
	yCell := cryptocell.NewChaCha20Poly1305(x.Cell, secret[:])
	return Volume{
		Cell:     yCell,
		FSStore:  x.FSStore,
		VCStore:  x.VCStore,
		RawStore: x.RawStore,
	}
}

func (r *CryptoSpace) handleDecryptFailure(x string, err error) {
	r.log.Debugf("decrypt failure %v: %v", x, err)
}

func deriveKey(out []byte, secret *[32]byte, purpose string) {
	gdat.DeriveKey(out, secret, []byte(purpose))
}

func readRandom(out []byte) {
	if _, err := rand.Read(out); err != nil {
		panic(err)
	}
}

var enc = base64.URLEncoding

func padBytes(x []byte, blockSize int) []byte {
	if blockSize > 255 {
		panic("cannot pad with blocksize more than 255")
	}
	extra := blockSize - (len(x)+1)%blockSize
	for i := 0; i < extra; i++ {
		x = append(x, 0x00)
	}
	return append(x, uint8(extra+1))
}

func unpadBytes(x []byte, blockSize int) ([]byte, error) {
	if len(x) < 1 {
		return nil, errors.Errorf("bytes len=%d is not padded", len(x))
	}
	extra := int(x[len(x)-1])
	end := len(x) - extra
	if end < 0 {
		return nil, errors.Errorf("bytes incorrectly padded")
	}
	return x[:end], nil
}
