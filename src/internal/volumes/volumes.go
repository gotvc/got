package volumes

import (
	"context"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state/cadata"
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
	Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error
	Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error)
	MaxSize() int
	Hash(data []byte) blobcache.CID
}

func Modify(ctx context.Context, vol Volume, fn func(dst stores.RW, x []byte) ([]byte, error)) error {
	tx, err := vol.BeginTx(ctx, TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	var x []byte
	if err := tx.Load(ctx, &x); err != nil {
		return err
	}
	y, err := fn(tx, x)
	if err != nil {
		return err
	}
	if err := tx.Save(ctx, y); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func View(ctx context.Context, vol Volume, fn func(src cadata.Getter, root []byte) error) error {
	tx, err := vol.BeginTx(ctx, TxParams{Mutate: false})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		return err
	}
	return fn(tx, root)
}

// Blobcache is a volume backed by blobcache.
type Blobcache struct {
	Service blobcache.Service
	Handle  blobcache.Handle
}

func (bc *Blobcache) BeginTx(ctx context.Context, tp TxParams) (Tx, error) {
	return bcsdk.BeginTx(ctx, bc.Service, bc.Handle, tp)
}
