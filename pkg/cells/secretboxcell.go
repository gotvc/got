package cells

import (
	"bytes"
	"context"
	"crypto/rand"

	"github.com/pkg/errors"
	"golang.org/x/crypto/nacl/secretbox"
)

type SecretBoxCell struct {
	inner  Cell
	secret []byte
}

func NewSecretBox(inner Cell, secret []byte) *SecretBoxCell {
	return &SecretBoxCell{
		inner:  inner,
		secret: secret,
	}
}

func (c *SecretBoxCell) Get(ctx context.Context) ([]byte, error) {
	data, _, err := c.get(ctx)
	return data, err
}

func (c *SecretBoxCell) get(ctx context.Context) (data, ctext []byte, err error) {
	ctext, err = c.inner.Get(ctx)
	if err != nil {
		return nil, nil, err
	}
	if len(ctext) == 0 {
		return nil, nil, nil
	}
	ptext, err := decrypt(ctext, c.secret)
	if err != nil {
		return nil, ctext, err
	}
	return ptext, ctext, nil
}

func (c *SecretBoxCell) CAS(ctx context.Context, prev, next []byte) (bool, []byte, error) {
	data, ctext, err := c.get(ctx)
	if err != nil {
		return false, nil, err
	}
	if bytes.Compare(data, prev) != 0 {
		return false, data, nil
	}
	nextCtext := encrypt(next, c.secret)
	swapped, actualCtext, err := c.inner.CAS(ctx, ctext, nextCtext)
	if err != nil {
		return false, nil, err
	}
	actual, err := decrypt(actualCtext, c.secret)
	if err != nil {
		return false, nil, err
	}
	return swapped, actual, nil
}

func encrypt(ptext, secret []byte) []byte {
	nonce := [24]byte{}
	if _, err := rand.Read(nonce[:]); err != nil {
		panic(err)
	}
	s := [32]byte{}
	copy(s[:], secret)
	return secretbox.Seal(nonce[:], ptext, &nonce, &s)
}

func decrypt(ctext, secret []byte) ([]byte, error) {
	if len(ctext) < 24 {
		return nil, errors.Errorf("secret box too short")
	}
	nonce := [24]byte{}
	copy(nonce[:], ctext[:24])
	s := [32]byte{}
	copy(s[:], secret)
	ptext, success := secretbox.Open([]byte{}, ctext, &nonce, &s)
	if !success {
		return nil, errors.Errorf("secret box was invalid")
	}
	return ptext, nil
}
