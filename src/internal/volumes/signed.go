package volumes

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/cloudflare/circl/sign"
	"go.inet256.org/inet256/src/inet256"
)

type GetVerifierFunc = func(context.Context, inet256.ID) (sign.PublicKey, error)

type SignedVolume struct {
	inner       Volume
	pki         inet256.PKI
	getVerifier GetVerifierFunc
	privateKey  inet256.PrivateKey
}

// NewSignedVolume creates a SignedVolume.
// publicKey must not be nil, if privateKey is nil, then the volume will be read only.
func NewSignedVolume(inner Volume, pki inet256.PKI, privateKey inet256.PrivateKey, getVerifier GetVerifierFunc) *SignedVolume {
	if getVerifier == nil {
		panic("getVerifier cannot be nil")
	}
	return &SignedVolume{
		inner:       inner,
		pki:         pki,
		privateKey:  privateKey,
		getVerifier: getVerifier,
	}
}

func (v *SignedVolume) BeginTx(ctx context.Context, tp TxParams) (Tx, error) {
	inner, err := v.inner.BeginTx(ctx, tp)
	if err != nil {
		return nil, err
	}
	return &SignedTx{
		inner:       inner,
		pki:         v.pki,
		privateKey:  v.privateKey,
		getVerifier: v.getVerifier,
	}, nil
}

type SignedTx struct {
	inner       Tx
	pki         inet256.PKI
	privateKey  inet256.PrivateKey
	getVerifier GetVerifierFunc
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
	if len(*dst) < 32 {
		return fmt.Errorf("too short to contain inet256.ID")
	}
	idBytes := (*dst)[len(*dst)-32:]
	*dst = (*dst)[:len(*dst)-32]
	id := inet256.IDFromBytes(idBytes)
	pubKey, err := tx.getVerifier(ctx, id)
	if err != nil {
		return fmt.Errorf("looking up verifier %v: %w", id, err)
	}

	sch := pubKey.Scheme()
	sigSize := sch.SignatureSize()
	if len(*dst) < sigSize {
		return fmt.Errorf("too small to contain signature")
	}
	msg, sig := (*dst)[:len(*dst)-sigSize], (*dst)[len(*dst)-sigSize:]
	if !tx.pki.Verify(&sigCtxVolume, pubKey, msg, sig) {
		return fmt.Errorf("invalid signature")
	}
	*dst = msg
	return nil
}

func (tx *SignedTx) Save(ctx context.Context, src []byte) error {
	if tx.privateKey == nil {
		return fmt.Errorf("private key not set, cannot create signature")
	}
	pubKey := tx.privateKey.Public().(sign.PublicKey)
	id := tx.pki.NewID(pubKey)
	// first append the signature
	src = tx.pki.Sign(&sigCtxVolume, tx.privateKey, src, src)
	// then append the ID
	src = append(src, id[:]...)
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
