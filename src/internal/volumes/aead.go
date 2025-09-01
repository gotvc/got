package volumes

import (
	"context"
	"crypto/cipher"

	"blobcache.io/blobcache/src/blobcache"
	"golang.org/x/crypto/chacha20poly1305"
)

type AEADVolume struct {
	inner Volume
	aead  cipher.AEAD
}

func NewAEAD(inner Volume, secret *[32]byte) *AEADVolume {
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
	return &AEADTx{tx: tx, aead: v.aead}, nil
}

type AEADTx struct {
	tx   Tx
	aead cipher.AEAD
}

func (tx *AEADTx) Commit(ctx context.Context) error {
	// TODO: seal root
	return tx.tx.Commit(ctx)
}

func (tx *AEADTx) Abort(ctx context.Context) error {
	return tx.tx.Abort(ctx)
}

func (tx *AEADTx) Load(ctx context.Context, dst *[]byte) error {
	// TODO: unseal dst
	return tx.tx.Load(ctx, dst)
}

func (tx *AEADTx) Save(ctx context.Context, root []byte) error {
	return tx.tx.Save(ctx, root)
}

func (tx *AEADTx) Post(ctx context.Context, data []byte) (cid blobcache.CID, err error) {
	return tx.tx.Post(ctx, data)
}

func (tx *AEADTx) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	return tx.tx.Exists(ctx, cid)
}

func (tx *AEADTx) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return tx.tx.Get(ctx, cid, buf)
}

func (tx *AEADTx) MaxSize() int {
	return tx.tx.MaxSize()
}

func (tx *AEADTx) Hash(data []byte) blobcache.CID {
	return tx.tx.Hash(data)
}
