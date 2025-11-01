package volumes

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/cloudflare/circl/sign"
	"github.com/cloudflare/circl/sign/mldsa/mldsa87"
	"go.inet256.org/inet256/src/inet256"
)

type SignedVolume struct {
	inner      Volume
	publicKey  inet256.PublicKey
	privateKey inet256.PrivateKey
}

// NewSignedVolume creates a SignedVolume.
// publicKey must not be nil, if privateKey is nil, then the volume will be read only.
func NewSignedVolume(inner Volume, publicKey inet256.PublicKey, privateKey inet256.PrivateKey) *SignedVolume {
	if publicKey == nil {
		panic("public key cannot be nil")
	}
	return &SignedVolume{
		inner:      inner,
		privateKey: privateKey,
		publicKey:  publicKey,
	}
}

func (v *SignedVolume) BeginTx(ctx context.Context, tp TxParams) (Tx, error) {
	inner, err := v.inner.BeginTx(ctx, tp)
	if err != nil {
		return nil, err
	}
	return &SignedTx{
		inner:      inner,
		privateKey: v.privateKey,
		publicKey:  v.publicKey,
	}, nil
}

type SignedTx struct {
	inner      Tx
	privateKey inet256.PrivateKey
	publicKey  inet256.PublicKey
}

func (tx *SignedTx) Commit(ctx context.Context) error {
	return tx.inner.Commit(ctx)
}

func (tx *SignedTx) Abort(ctx context.Context) error {
	return tx.inner.Abort(ctx)
}

func (tx *SignedTx) Load(ctx context.Context, dst *[]byte) error {
	if err := tx.inner.Load(ctx, dst); err != nil {
		return err
	}
	if len(*dst) == 0 {
		return nil
	}
	sch := tx.publicKey.Scheme()
	sigSize := sch.SignatureSize()
	if len(*dst) < sigSize {
		return fmt.Errorf("too small to contain signature")
	}
	msg, sig := (*dst)[:len(*dst)-sigSize], (*dst)[len(*dst)-sigSize:]
	if !pki.Verify(&sigCtxVolume, tx.publicKey, msg, sig) {
		return fmt.Errorf("invalid signature")
	}
	*dst = msg
	return nil
}

func (tx *SignedTx) Save(ctx context.Context, src []byte) error {
	if tx.privateKey == nil {
		return fmt.Errorf("private key not set, cannot create signature")
	}
	src = pki.Sign(&sigCtxVolume, tx.privateKey, src, src)
	return tx.inner.Save(ctx, src)
}

func (tx *SignedTx) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	return tx.inner.Post(ctx, data)
}

func (tx *SignedTx) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return tx.inner.Exists(ctx, cids, dst)
}

func (tx *SignedTx) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return tx.inner.Get(ctx, cid, buf)
}

func (tx *SignedTx) MaxSize() int {
	return tx.inner.MaxSize()
}

func (tx *SignedTx) Hash(data []byte) blobcache.CID {
	return tx.inner.Hash(data)
}

var sigCtxVolume = inet256.SigCtxString("blobcache/volume-root")

var pki = inet256.PKI{
	Default: "mldsa87",
	Schemes: map[string]sign.Scheme{
		"mldsa87": mldsa87.Scheme(),
	},
}
