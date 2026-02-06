package gotcore

import (
	"context"
	"fmt"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
)

// MarkTx scopes down a SpaceTx to a single Mark.
type MarkTx struct {
	stx  SpaceTx
	name string
	info Info

	gotvc *VCMach
	gotfs *FSMach
}

func NewMarkTx(ctx context.Context, stx SpaceTx, name string) (*MarkTx, error) {
	info, err := stx.Inspect(ctx, name)
	if err != nil {
		return nil, err
	}
	return &MarkTx{
		stx:  stx,
		name: name,
		info: *info,
	}, nil
}

func (b *MarkTx) init() {
	if b.gotvc == nil {
		b.gotvc = newGotVC(&b.info.Config)
	}
	if b.gotfs == nil {
		b.gotfs = newGotFS(&b.info.Config)
	}
}

func (b *MarkTx) Info() Info {
	return b.info
}

func (b *MarkTx) GotFS() *gotfs.Machine {
	b.init()
	return b.gotfs
}

func (b *MarkTx) GotVC() *VCMach {
	b.init()
	return b.gotvc
}

func (m *MarkTx) Config() DSConfig {
	return m.info.Config
}

func (mtx *MarkTx) FSRO() [2]stores.Reading {
	ss := mtx.RO()
	return [2]stores.Reading{ss[0], ss[1]}
}

func (mtx *MarkTx) FSRW() [2]stores.RW {
	ss := mtx.stx.Stores()
	return [2]stores.RW{ss[0], ss[1]}
}

func (mtx *MarkTx) VCRO() stores.Reading {
	ss := mtx.RO()
	return ss[2]
}

func (mtx *MarkTx) VCRW() stores.RW {
	ss := mtx.stx.Stores()
	return ss[2]
}

func (mtx *MarkTx) RO() [3]stores.Reading {
	ss := mtx.stx.Stores()
	return [3]stores.Reading{
		ss[0],
		ss[1],
		ss[2],
	}
}

func (mtx *MarkTx) WO() [3]stores.Writing {
	ss := mtx.stx.Stores()
	return [3]stores.Writing{
		ss[0],
		ss[1],
		ss[2],
	}
}

// Save saves the snapshot to the Mark
func (m *MarkTx) Save(ctx context.Context, ref gdat.Ref) error {
	ss := m.stx.Stores()
	if !ref.IsZero() {
		if exists, err := stores.ExistsUnit(ctx, ss[2], ref.CID); err != nil {
			return err
		} else if !exists {
			return ErrRefIntegrity{Ref: ref, Store: "gotvc"}
		}
	}
	return m.stx.SetTarget(ctx, m.name, ref)
}

// Load loads a snapshot from the Mark
func (b *MarkTx) Load(ctx context.Context, dst *gdat.Ref) (bool, error) {
	return b.stx.GetTarget(ctx, b.name, dst)
}

func (mt *MarkTx) LoadSnap(ctx context.Context, dst *Snap) (bool, error) {
	var ref gdat.Ref
	if ok, err := mt.Load(ctx, &ref); err != nil {
		return false, err
	} else if ok {
		snap, err := mt.GotVC().GetSnapshot(ctx, mt.VCRO(), ref)
		if err != nil {
			return false, err
		}
		*dst = *snap
		return true, nil
	} else {
		return false, nil
	}
}

// Apply calls fn with the Marks target Ref
// If the mark does not yet have a target, then the ref will be 0.
func (m *MarkTx) Apply(ctx context.Context, fn func([3]stores.RW, gdat.Ref) (gdat.Ref, error)) error {
	var yRef gdat.Ref
	var xRef gdat.Ref
	if _, err := m.Load(ctx, &xRef); err != nil {
		return err
	}
	yRef, err := fn(m.stx.Stores(), xRef)
	if err != nil {
		return err
	}
	return m.Save(ctx, yRef)
}

func (m *MarkTx) Modify(ctx context.Context, fn func(mctx ModifyCtx) (*Snap, error)) error {
	m.init()
	ss := m.stx.Stores()
	var snap *Snap
	var target gdat.Ref
	if ok, err := m.stx.GetTarget(ctx, m.name, &target); err != nil {
		return err
	} else if ok {
		snap, err = m.GotVC().GetSnapshot(ctx, ss[2], target)
		if err != nil {
			return err
		}
	}
	modctx := ModifyCtx{
		VC:     m.GotVC(),
		FS:     m.GotFS(),
		Stores: ss,
		Root:   snap,
	}
	y, err := fn(modctx)
	if err != nil {
		return err
	}
	var yRef gdat.Ref
	if y != nil {
		ref, err := m.GotVC().PostSnapshot(ctx, m.VCRW(), *y)
		if err != nil {
			return err
		}
		ref, err = syncSnapRef(ctx, m.gotvc, m.gotfs, m.RO(), m.WO(), *ref)
		if err != nil {
			return err
		}
		yRef = *ref
	}
	return m.Save(ctx, yRef)
}

// ModifyCtx is the context passed to the modify function.
type ModifyCtx struct {
	VC     *VCMach
	FS     *FSMach
	Stores [3]stores.RW
	Root   *Snap
}

// Sync syncs a snapshot into the store
func (mctx *ModifyCtx) Sync(ctx context.Context, srcs [3]stores.Reading, root Snap) error {
	return mctx.VC.Sync(ctx, srcs[2], mctx.Stores[2], root, func(payload Payload) error {
		return mctx.FS.Sync(ctx,
			[2]stores.Reading{srcs[0], srcs[1]},
			[2]stores.Writing{mctx.Stores[0], mctx.Stores[1]},
			payload.Root,
		)
	})
}

func (b *MarkTx) History(ctx context.Context, fn func(ref gdat.Ref, snap Snap) error) error {
	b.init()
	var snap Snap
	if ok, err := b.LoadSnap(ctx, &snap); err != nil {
		return err
	} else if !ok {
		return nil
	}
	ref := b.gotvc.RefFromSnapshot(snap)
	if err := fn(ref, snap); err != nil {
		return err
	}
	return b.gotvc.ForEach(ctx, b.VCRO(), snap.Parents, fn)
}

func (b *MarkTx) LoadFS(ctx context.Context, dst *gotfs.Root) (bool, error) {
	b.init()
	var snap Snap
	if ok, err := b.LoadSnap(ctx, &snap); err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}
	*dst = snap.Payload.Root
	return true, nil
}

type ViewCtx struct {
	VC     *VCMach
	FS     *FSMach
	Stores [3]stores.Reading
	Root   *Snap
}

func (vc *ViewCtx) FSRO() [2]stores.Reading {
	return [2]stores.Reading{vc.Stores[0], vc.Stores[1]}
}

// ViewSnapshot calls fn with everything needed to read a Snapshot, its filesystem, and its ancestry.
func ViewSnapshot(ctx context.Context, stx SpaceTx, se SnapExpr, fn func(vctx *ViewCtx) error) error {
	ref, err := se.Resolve(ctx, stx)
	if err != nil {
		return err
	}
	ss := stx.Stores()
	cfg := DefaultConfig(false)
	fsmach := newGotFS(&cfg)
	vcmach := newGotVC(&cfg)
	snap, err := vcmach.GetSnapshot(ctx, ss[2], *ref)
	if err != nil {
		return err
	}
	vctx := ViewCtx{
		VC:     vcmach,
		FS:     fsmach,
		Stores: [3]stores.Reading{ss[0], ss[1], ss[2]},
		Root:   snap,
	}
	return fn(&vctx)
}

// SyncVolumes syncs the contents of src to dst.
func Sync(ctx context.Context, src, dst *MarkTx, force bool) error {
	return dst.Apply(ctx, func(dsts [3]stores.RW, x gdat.Ref) (gdat.Ref, error) {
		var goal *Snap
		var goalRef gdat.Ref
		if ok, err := src.Load(ctx, &goalRef); err != nil {
			return gdat.Ref{}, err
		} else if ok {
			var err error
			goal, err = src.GotVC().GetSnapshot(ctx, src.VCRO(), goalRef)
			if err != nil {
				return gdat.Ref{}, err
			}
		}

		switch {
		case goal == nil && x.IsZero():
			return gdat.Ref{}, nil
		case goal == nil && !force:
			return gdat.Ref{}, fmt.Errorf("cannot clear volume without force=true")
		case x.IsZero():
		case goalRef.Equals(&x):
		default:
			vcmach := dst.GotVC()
			prevSnap, err := vcmach.GetSnapshot(ctx, dst.VCRO(), x)
			if err != nil {
				return gdat.Ref{}, err
			}
			hasAncestor, err := vcmach.IsDescendentOf(ctx, src.VCRO(), *goal, *prevSnap)
			if err != nil {
				return gdat.Ref{}, err
			}
			if !force && !hasAncestor {
				return gdat.Ref{}, fmt.Errorf("cannot CAS, dst ref is not parent of src ref")
			}
		}
		ref, err := syncSnapRef(ctx, dst.GotVC(), dst.GotFS(), src.RO(), dst.WO(), goalRef)
		if err != nil {
			return gdat.Ref{}, err
		}
		return *ref, nil
	})
}

func History(ctx context.Context, vcmach *VCMach, s stores.Reading, snap Snap, fn func(ref gdat.Ref, snap Snap) error) error {
	ref := vcmach.RefFromSnapshot(snap)
	if err := fn(ref, snap); err != nil {
		return err
	}
	return vcmach.ForEach(ctx, s, snap.Parents, fn)
}

// syncSnapRef ensures that all content reachable from Ref is in the dst store.
// blobs are copied from the source store as needed.
func syncSnapRef(ctx context.Context, vcmach *VCMach, fsmach *gotfs.Machine, src [3]stores.Reading, dst [3]stores.Writing, ref gdat.Ref) (_ *gdat.Ref, err error) {
	ctx, cf := metrics.Child(ctx, "syncing gotvc")
	defer cf()
	snap, err := vcmach.GetSnapshot(ctx, src[2], ref)
	if err != nil {
		return nil, err
	}
	if err := vcmach.Sync(ctx, src[2], dst[2], *snap, func(payload Payload) error {
		return fsmach.Sync(ctx, [2]stores.Reading{src[0], src[1]}, [2]stores.Writing{dst[0], dst[1]}, payload.Root)
	}); err != nil {
		return nil, err
	}
	return vcmach.PostSnapshot(ctx, dst[2], *snap)
}

// NewGotFS creates a new gotfs.Machine suitable for writing to the mark
func newGotFS(b *DSConfig) *gotfs.Machine {
	opts := append([]gotfs.Option{}, gotfs.WithSalt(deriveFSSalt(b)))
	fsag := gotfs.NewMachine(opts...)
	return fsag
}

// NewGotVC creates a new gotvc.Machine suitable for writing to the mark
func newGotVC(b *DSConfig) *VCMach {
	return gotvc.NewMachine(ParsePayload, gotvc.Config{
		Salt: *deriveVCSalt(b),
	})
}

func deriveFSSalt(b *DSConfig) *[32]byte {
	var out [32]byte
	gdat.DeriveKey(out[:], (*[32]byte)(&b.Salt), []byte("gotfs"))
	return &out
}

func deriveVCSalt(b *DSConfig) *[32]byte {
	var out [32]byte
	gdat.DeriveKey(out[:], (*[32]byte)(&b.Salt), []byte("gotvc"))
	return &out
}
