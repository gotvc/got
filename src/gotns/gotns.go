package gotns

import (
	"context"
	"encoding/json"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotkv/kvstreams"
	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
	"go.brendoncarroll.net/exp/streams"
)

func BeginTx(ctx context.Context, dmach *gdat.Machine, kvmach *gotkv.Machine, vol volumes.Volume, modify bool) (*Tx, error) {
	tx, err := vol.BeginTx(ctx, blobcache.TxParams{Modify: true})
	if err != nil {
		return nil, err
	}
	return &Tx{
		tx:     tx,
		dmach:  dmach,
		kvmach: kvmach,
	}, nil
}

// Root is the root of a gotns namespace
type Root struct {
	Branches gotkv.Root
}

func ParseRoot(data []byte) (*Root, error) {
	var r Root
	if err := r.Unmarshal(data); err != nil {
		return nil, err
	}
	return &r, nil
}

func (r *Root) Unmarshal(data []byte) error {
	kvrData, _, err := sbe.ReadLP16(data)
	if err != nil {
		return err
	}
	return r.Branches.Unmarshal(kvrData)
}

func (r Root) Marshal(out []byte) []byte {
	return sbe.AppendLP16(nil, r.Branches.Marshal(nil))
}

type BranchState struct {
	Info branches.Info `json:"info"`
	To   gdat.Ref      `json:"to"`
}

func (b BranchState) Marshal(out []byte) []byte {
	data, err := json.Marshal(b)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (b *BranchState) Unmarshal(data []byte) error {
	if err := json.Unmarshal(data, b); err != nil {
		return err
	}
	return nil
}

// Tx is a transaction on a Namespace
type Tx struct {
	tx     volumes.Tx
	dmach  *gdat.Machine
	kvmach *gotkv.Machine

	kvtx *gotkv.Tx
}

func (tx *Tx) loadKV(ctx context.Context) error {
	if tx.kvtx != nil {
		return nil
	}
	var root []byte
	if err := tx.tx.Load(ctx, &root); err != nil {
		return err
	}
	var kvr gotkv.Root
	if len(root) == 0 {
		r, err := tx.kvmach.NewEmpty(ctx, tx.tx)
		if err != nil {
			return err
		}
		kvr = *r
	} else {
		if err := kvr.Unmarshal(root); err != nil {
			return err
		}
	}

	tx.kvtx = tx.kvmach.NewTx(tx.tx, kvr)
	return nil
}

func (tx *Tx) Get(ctx context.Context, name string) (*BranchState, error) {
	if err := tx.loadKV(ctx); err != nil {
		return nil, err
	}
	var val []byte
	if found, err := tx.kvtx.Get(ctx, []byte(name), &val); err != nil {
		return nil, err
	} else if !found {
		return nil, nil
	}
	var b BranchState
	if err := b.Unmarshal(val); err != nil {
		return nil, err
	}
	return &b, nil
}

func (tx *Tx) Put(ctx context.Context, name string, b BranchState) error {
	if err := tx.loadKV(ctx); err != nil {
		return err
	}
	if err := branches.CheckName(name); err != nil {
		return err
	}
	return tx.kvtx.Put(ctx, []byte(name), b.Marshal(nil))
}

func (tx *Tx) Delete(ctx context.Context, name string) error {
	if err := tx.loadKV(ctx); err != nil {
		return err
	}
	return tx.kvtx.Delete(ctx, []byte(name))
}

func (tx *Tx) ListNames(ctx context.Context, span branches.Span, limit int) ([]string, error) {
	if err := tx.loadKV(ctx); err != nil {
		return nil, err
	}
	_, err := tx.kvtx.Flush(ctx)
	if err != nil {
		return nil, err
	}
	span2 := gotkv.TotalSpan()
	it := tx.kvtx.Iterate(ctx, span2)
	//it := tx.kvmach.NewIterator(tx.tx, *root, span2)
	it2 := kvstreams.NewMap(it, func(dst *string, x gotkv.Entry) {
		*dst = string(x.Key)
	})
	if limit < 1 {
		limit = 1024
	}
	return streams.Collect(ctx, it2, limit)
}

func (tx *Tx) SaveBranchRoot(ctx context.Context, name string, data []byte) error {
	b, err := tx.Get(ctx, name)
	if err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("cannot load branch root for %s; it does not exist", name)
	}
	ref, err := tx.dmach.Post(ctx, tx.tx, data)
	if err != nil {
		return err
	}
	b.To = *ref
	return tx.Put(ctx, name, *b)
}

func (tx *Tx) LoadBranchRoot(ctx context.Context, name string, dst *[]byte) error {
	b, err := tx.Get(ctx, name)
	if err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("cannot load branch root for %s; it does not exist", name)
	}
	return tx.dmach.GetF(ctx, tx.tx, b.To, func(data []byte) error {
		*dst = append((*dst)[0:], data...)
		return nil
	})
}

func (tx *Tx) Abort(ctx context.Context) error {
	if tx.tx == nil {
		return nil
	}
	return tx.tx.Abort(ctx)
}

func (tx *Tx) Commit(ctx context.Context) error {
	if tx.kvtx == nil {
		return fmt.Errorf("tx is already done")
	}
	kvroot, err := tx.kvtx.Flush(ctx)
	if err != nil {
		return err
	}
	r := Root{Branches: *kvroot}
	if err := saveRoot(ctx, tx.tx, r); err != nil {
		return err
	}
	if err := tx.tx.Commit(ctx); err != nil {
		return err
	}
	tx.tx = nil
	return nil
}

func saveRoot(ctx context.Context, tx volumes.Tx, r Root) error {
	return tx.Save(ctx, r.Marshal(nil))
}

func DefaultVolumeSpec() blobcache.VolumeSpec {
	spec := blobcache.DefaultLocalSpec()
	spec.Local.HashAlgo = blobcache.HashAlgo_BLAKE2b_256
	spec.Local.MaxSize = stores.MaxSize
	return spec
}
