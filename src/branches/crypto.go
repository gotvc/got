package branches

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/exp/slices"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/cells"
)

const SecretSize = 32

const (
	purposeBranchNames = "got/space/names"
	paddingBlockSize   = 16
)

var codec = base64.NewEncoding(gdat.Base64Alphabet)

// CryptoSpaceOptions configure a CryptoSpace
type CryptoSpaceOption = func(*CryptoSpace)

// WithDecryptFailureHandler sets fn to be called by the space when there is a decryption failure.
func WithDecryptFailureHandler(fn func(string, error)) CryptoSpaceOption {
	return func(cs *CryptoSpace) {
		cs.onDecryptFail = fn
	}
}

func WithPassthrough(branches []string) CryptoSpaceOption {
	return func(cs *CryptoSpace) {
		cs.passthrough = append([]string{}, branches...)
	}
}

type CryptoSpace struct {
	inner         Space
	secret        *[32]byte
	onDecryptFail func(string, error)
	passthrough   []string
}

func NewCryptoSpace(inner Space, secret *[32]byte, opts ...CryptoSpaceOption) Space {
	s := &CryptoSpace{
		secret:        secret,
		inner:         inner,
		onDecryptFail: func(string, error) {},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (r *CryptoSpace) Create(ctx context.Context, name string, cfg Config) (*Info, error) {
	if r.shouldPassthrough(name) {
		return r.inner.Create(ctx, name, cfg)
	}
	nameCtext := r.encryptName(name)
	info2 := r.encryptInfo(cfg.AsInfo())
	return r.inner.Create(ctx, nameCtext, info2.AsConfig())
}

func (r *CryptoSpace) Set(ctx context.Context, name string, cfg Config) error {
	if r.shouldPassthrough(name) {
		return r.inner.Set(ctx, name, cfg)
	}
	nameCtext := r.encryptName(name)
	infoCtext := r.encryptInfo(cfg.AsInfo())
	return r.inner.Set(ctx, nameCtext, infoCtext.AsConfig())
}

func (r *CryptoSpace) Get(ctx context.Context, name string) (*Info, error) {
	if r.shouldPassthrough(name) {
		return r.inner.Get(ctx, name)
	}
	nameCtext := r.encryptName(name)
	info, err := r.inner.Get(ctx, nameCtext)
	if err != nil {
		return nil, err
	}
	return r.decryptInfo(*info)
}

func (r *CryptoSpace) Delete(ctx context.Context, name string) error {
	if r.shouldPassthrough(name) {
		return r.inner.Delete(ctx, name)
	}
	nameCtext := r.encryptName(name)
	return r.inner.Delete(ctx, nameCtext)
}

func (r *CryptoSpace) List(ctx context.Context, span Span, limit int) (ret []string, _ error) {
	err := ForEach(ctx, r.inner, TotalSpan(), func(x string) error {
		if r.shouldPassthrough(x) {
			ret = append(ret, x)
			return nil
		}
		y, err := r.decryptName(x)
		if err != nil {
			r.onDecryptFail(x, err)
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

func (r *CryptoSpace) Open(ctx context.Context, name string) (*Volume, error) {
	v, err := r.inner.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return r.wrapVolume(name, v), nil
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
	return fmt.Sprintf("%s.%s", codec.EncodeToString(nonce[:]), codec.EncodeToString(ctext[:]))
}

func (r *CryptoSpace) decryptName(x string) (string, error) {
	parts := bytes.SplitN([]byte(x), []byte{'.'}, 2)
	if len(parts) < 2 {
		return "", fmt.Errorf("missing nonce")
	}
	var nonce [24]byte
	if _, err := codec.Decode(nonce[:], parts[0]); err != nil {
		return "", err
	}
	ctext := make([]byte, codec.DecodedLen(len(parts[1])))
	n, err := codec.Decode(ctext, parts[1])
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

func (r *CryptoSpace) encryptInfo(x Info) Info {
	y := Info{
		Salt: r.encryptSalt(x.Salt),
		Mode: x.Mode,
	}
	SortAnnotations(y.Annotations)
	return y
}

func (r *CryptoSpace) decryptInfo(x Info) (*Info, error) {
	salt, err := r.decryptSalt(x.Salt)
	if err != nil {
		return nil, err
	}
	return &Info{
		Salt: salt,
		Mode: x.Mode,
	}, nil
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
		return nil, fmt.Errorf("salt ctext not long enough to contain nonce len=%d", len(x))
	}
	nonce := x[:24]
	ctext := x[24:]
	return r.getAEAD(secret[:]).Open(nil, nonce, ctext, nil)
}

func (r *CryptoSpace) wrapVolume(name string, x *Volume) *Volume {
	var secret [32]byte
	deriveKey(secret[:], r.secret, "got/space/cells/"+name)
	yCell := cells.NewEncrypted(x.Cell, &secret)
	return &Volume{
		Cell:     yCell,
		FSStore:  x.FSStore,
		VCStore:  x.VCStore,
		RawStore: x.RawStore,
	}
}

func (r *CryptoSpace) shouldPassthrough(name string) bool {
	return slices.Contains(r.passthrough, name)
}

func deriveKey(out []byte, secret *[32]byte, purpose string) {
	gdat.DeriveKey(out, secret, []byte(purpose))
}

func readRandom(out []byte) {
	if _, err := rand.Read(out); err != nil {
		panic(err)
	}
}

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
		return nil, fmt.Errorf("bytes len=%d is not padded", len(x))
	}
	extra := int(x[len(x)-1])
	end := len(x) - extra
	if end < 0 {
		return nil, fmt.Errorf("bytes incorrectly padded")
	}
	return x[:end], nil
}
