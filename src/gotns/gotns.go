package gotns

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/marks"
	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
	"go.brendoncarroll.net/exp/streams"
)

func BeginTx(ctx context.Context, dmach *gdat.Machine, kvmach *gotkv.Machine, vol volumes.Volume, modify bool) (*Tx, error) {
	ctx, cf := context.WithTimeoutCause(ctx, 3*time.Second, errors.New("trying to begin transaction"))
	defer cf()
	tx, err := vol.BeginTx(ctx, blobcache.TxParams{Modify: true})
	if err != nil {
		return nil, err
	}
	return NewTx(tx, dmach, kvmach), nil
}

func NewGotKV() gotkv.Machine {
	return gotkv.NewMachine(1<<13, 1<<18)
}

func NewTx(tx volumes.Tx, dmach *gdat.Machine, kvmach *gotkv.Machine) *Tx {
	return &Tx{
		tx:     tx,
		dmach:  dmach,
		kvmach: kvmach,
	}
}

// Root is the root of a gotns namespace
type Root struct {
	Marks gotkv.Root
	// BrokenSet is the set of CIDs which the Space cannot dereference.
	// This is used to correctly Sync even with incomplete structures.
	BrokenSet gotkv.Root
}

func ParseRoot(data []byte) (*Root, error) {
	var r Root
	if err := r.Unmarshal(data); err != nil {
		return nil, err
	}
	return &r, nil
}

func (r *Root) Unmarshal(data []byte) error {
	kvrData, data, err := sbe.ReadLP16(data)
	if err != nil {
		return err
	}
	bsData, _, err := sbe.ReadLP16(data)
	if err != nil {
		return err
	}
	if err := r.Marks.Unmarshal(kvrData); err != nil {
		return err
	}
	if err := r.BrokenSet.Unmarshal(bsData); err != nil {
		return err
	}
	return nil
}

func (r Root) Marshal(out []byte) []byte {
	out = sbe.AppendLP16(out, r.Marks.Marshal(nil))
	out = sbe.AppendLP16(out, r.BrokenSet.Marshal(nil))
	return out
}

type MarkState struct {
	// Info is stored as json
	Info marks.Info `json:"info"`
	// Target is stored as json.
	Target gdat.Ref `json:"target"`
}

func (b MarkState) Marshal(out []byte) []byte {
	infoJSON, err := json.Marshal(b.Info)
	if err != nil {
		panic(err)
	}
	out = append(out, infoJSON...)
	out = append(out, b.Target.Marshal()...)
	return out
}

func (s *MarkState) Unmarshal(data []byte) error {
	infoJSON := data[:len(data)-gdat.RefSize]
	refData := data[len(data)-gdat.RefSize:]
	if err := s.Target.Unmarshal(refData); err != nil {
		return err
	}
	if err := json.Unmarshal(infoJSON, &s.Info); err != nil {
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
	if tx.tx == nil {
		return fmt.Errorf("tx is already done")
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
		r, err := ParseRoot(root)
		if err != nil {
			return err
		}
		kvr = r.Marks
	}
	tx.kvtx = tx.kvmach.NewTx(tx.tx, kvr)
	return nil
}

func (tx *Tx) Get(ctx context.Context, name string) (*MarkState, error) {
	if err := tx.loadKV(ctx); err != nil {
		return nil, err
	}
	var val []byte
	if found, err := tx.kvtx.Get(ctx, []byte(name), &val); err != nil {
		return nil, err
	} else if !found {
		return nil, nil
	}
	var ms MarkState
	if err := ms.Unmarshal(val); err != nil {
		return nil, err
	}
	return &ms, nil
}

func (tx *Tx) Put(ctx context.Context, name string, b MarkState) error {
	if err := tx.loadKV(ctx); err != nil {
		return err
	}
	if err := marks.CheckName(name); err != nil {
		return err
	}
	if yes, err := stores.ExistsUnit(ctx, tx.tx, b.Target.CID); err != nil {
		return err
	} else if !yes {
		return fmt.Errorf("mark target not found: %v", b.Target.CID)
	}
	return tx.kvtx.Put(ctx, []byte(name), b.Marshal(nil))
}

func (tx *Tx) Delete(ctx context.Context, name string) error {
	if err := tx.loadKV(ctx); err != nil {
		return err
	}
	return tx.kvtx.Delete(ctx, []byte(name))
}

func (tx *Tx) ListNames(ctx context.Context, span marks.Span, limit int) ([]string, error) {
	if err := tx.loadKV(ctx); err != nil {
		return nil, err
	}
	_, err := tx.kvtx.Flush(ctx)
	if err != nil {
		return nil, err
	}
	span2 := gotkv.TotalSpan()
	if span.Begin != "" {
		span2.Begin = []byte(span.Begin)
	}
	if span.End != "" {
		span2.End = []byte(span.End)
	}
	it := tx.kvtx.Iterate(ctx, span2)
	//it := tx.kvmach.NewIterator(tx.tx, *root, span2)
	it2 := streams.NewMap(it, func(dst *string, x gotkv.Entry) {
		*dst = string(x.Key)
	})
	if limit < 1 {
		limit = 1024
	}
	return streams.Collect(ctx, it2, limit)
}

func (tx *Tx) SaveMarkRoot(ctx context.Context, name string, data []byte) error {
	m, err := tx.Get(ctx, name)
	if err != nil {
		return err
	}
	if m == nil {
		return fmt.Errorf("cannot load branch root for %s; it does not exist", name)
	}
	ref, err := tx.dmach.Post(ctx, tx.tx, data)
	if err != nil {
		return err
	}
	m.Target = *ref
	return tx.Put(ctx, name, *m)
}

func (tx *Tx) LoadBranchRoot(ctx context.Context, name string, dst *[]byte) error {
	b, err := tx.Get(ctx, name)
	if err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("cannot load branch root for %s; it does not exist", name)
	}
	return tx.dmach.GetF(ctx, tx.tx, b.Target, func(data []byte) error {
		*dst = append((*dst)[0:], data...)
		return nil
	})
}

func (tx *Tx) Abort(ctx context.Context) error {
	if tx.tx == nil {
		return nil
	}
	if err := tx.tx.Abort(ctx); err != nil {
		return err
	}
	tx.tx = nil
	tx.kvtx = nil
	return nil
}

func (tx *Tx) Commit(ctx context.Context) error {
	if tx.tx == nil {
		return fmt.Errorf("tx is already done")
	}
	kvroot, err := tx.kvtx.Flush(ctx)
	if err != nil {
		return err
	}
	bsroot, err := tx.kvmach.NewEmpty(ctx, tx.tx)
	if err != nil {
		return err
	}
	r := Root{Marks: *kvroot, BrokenSet: *bsroot}
	if err := saveRoot(ctx, tx.tx, r); err != nil {
		return err
	}
	if err := tx.tx.Commit(ctx); err != nil {
		return err
	}
	tx.tx = nil
	tx.kvtx = nil
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
