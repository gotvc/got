package volumes

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"golang.org/x/crypto/chacha20poly1305"
)

type AEADVolume struct {
	inner Volume
	aead  cipher.AEAD
}

// NewChaCha20Poly1305 creates a new AEAD volume that uses the ChaCha20Poly1305 algorithm.
// It uses the 24 byte nonce variant.
func NewChaCha20Poly1305(inner Volume, secret *[32]byte) *AEADVolume {
	aead, err := chacha20poly1305.NewX(secret[:])
	if err != nil {
		panic(err)
	}
	return &AEADVolume{
		inner: inner,
		aead:  aead,
	}
}

func (v *AEADVolume) BeginTx(ctx context.Context, tp TxParams) (Tx, error) {
	tx, err := v.inner.BeginTx(ctx, tp)
	if err != nil {
		return nil, err
	}
	return &AEADTx{inner: tx, aead: v.aead}, nil
}

type AEADTx struct {
	inner Tx
	aead  cipher.AEAD
}

func (tx *AEADTx) Commit(ctx context.Context) error {
	return tx.inner.Commit(ctx)
}

func (tx *AEADTx) Abort(ctx context.Context) error {
	return tx.inner.Abort(ctx)
}

func (tx *AEADTx) Load(ctx context.Context, dst *[]byte) error {
	if err := tx.inner.Load(ctx, dst); err != nil {
		return err
	}
	// as a special case if the plaintext is empty, then we return nil.
	if len(*dst) == 0 {
		*dst = (*dst)[:0]
		return nil
	}
	if len(*dst) < tx.aead.NonceSize() {
		return fmt.Errorf("too small to contain 24 byte nonce: %d", len(*dst))
	}
	nonce := (*dst)[:tx.aead.NonceSize()]
	ctext := (*dst)[tx.aead.NonceSize():]
	plaintext, err := tx.aead.Open(ctext[:0], nonce[:], ctext, nil)
	if err != nil {
		return err
	}
	*dst = plaintext
	return nil
}

func (tx *AEADTx) Save(ctx context.Context, ptext []byte) error {
	nonce := make([]byte, tx.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		panic(err)
	}
	ctext := tx.aead.Seal(nonce, nonce, ptext, nil)
	return tx.inner.Save(ctx, ctext)
}

func (tx *AEADTx) Post(ctx context.Context, data []byte) (cid blobcache.CID, err error) {
	return tx.inner.Post(ctx, data)
}

func (tx *AEADTx) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	return tx.inner.Exists(ctx, cid)
}

func (tx *AEADTx) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return tx.inner.Get(ctx, cid, buf)
}

func (tx *AEADTx) MaxSize() int {
	return tx.inner.MaxSize()
}

func (tx *AEADTx) Hash(data []byte) blobcache.CID {
	return tx.inner.Hash(data)
}
