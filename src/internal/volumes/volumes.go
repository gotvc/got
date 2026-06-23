package volumes

import (
	"context"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
)

type TxParams = blobcache.TxParams

type Volume interface {
	BeginTx(ctx context.Context, tp TxParams) (Tx, error)
}

var _ Tx = &bcsdk.Tx{}

type Tx interface {
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
	Load(ctx context.Context, dst *[]byte) error
	Save(ctx context.Context, src []byte) error

	Post(ctx context.Context, data []byte) (blobcache.CID, error)
	Exists(ctx context.Context, cids []blobcache.CID, dst *blobcache.BitMap) error
	Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error)
	MaxSize() int
	Hash(data []byte) blobcache.CID
}

// Blobcache is a volume backed by blobcache.
type Blobcache struct {
	Service blobcache.Service
	Handle  blobcache.Handle
}

func (bc *Blobcache) BeginTx(ctx context.Context, tp TxParams) (Tx, error) {
	return bcsdk.BeginTx(ctx, bc.Service, bc.Handle, tp)
}
