package marks

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
	return ss[0]
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
func (b *MarkTx) Save(ctx context.Context, snap *Snap) error {
	if snap == nil {
		var zeroRef gdat.Ref
		return b.stx.SetTarget(ctx, b.name, zeroRef)
	}
	ss := b.stx.Stores()
	ref, err := b.GotVC().PostSnapshot(ctx, ss[2], *snap)
	if err != nil {
		return err
	}
	return b.stx.SetTarget(ctx, b.name, *ref)
}

// Load loads a snapshot from the Mark
func (b *MarkTx) Load(ctx context.Context, dst *Snap) (bool, error) {
	var ref gdat.Ref
	ok, err := b.stx.GetTarget(ctx, b.name, &ref)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	ss := b.stx.Stores()
	snap, err := GetSnapshot(ctx, ss[2], ref)
	if err != nil {
		return false, err
	}
	*dst = *snap
	return true, nil
}

func (m *MarkTx) Apply(ctx context.Context, fn func([3]stores.RW, *Snap) (*Snap, error)) error {
	var x Snap
	var y *Snap
	if ok, err := m.Load(ctx, &x); err != nil {
		return err
	} else if !ok {
		y, err = fn(m.stx.Stores(), nil)
		if err != nil {
			return err
		}
	} else {
		y, err = fn(m.stx.Stores(), &x)
		if err != nil {
			return err
		}
	}
	return m.Save(ctx, y)
}

func (m *MarkTx) Modify(ctx context.Context, fn func(mctx ModifyCtx) (*Snap, error)) error {
	m.init()
	ss := m.stx.Stores()
	var snap *Snap
	var target gdat.Ref
	if ok, err := m.stx.GetTarget(ctx, m.name, &target); err != nil {
		return err
	} else if ok {
		snap, err = GetSnapshot(ctx, ss[2], target)
		if err != nil {
			return err
		}
		return nil
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
	if y != nil {
		if err := syncStores(ctx, m.gotvc, m.gotfs, m.RO(), m.WO(), *y); err != nil {
			return err
		}
	}
	if y != nil {
		ref, err := modctx.VC.PostSnapshot(ctx, ss[2], *y)
		if err != nil {
			return err
		}
		if err := m.stx.SetTarget(ctx, m.name, *ref); err != nil {
			return err
		}
	} else {
		var zeroRef gdat.Ref
		if err := m.stx.SetTarget(ctx, m.name, zeroRef); err != nil {
			return err
		}
	}
	return nil
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
	if ok, err := b.Load(ctx, &snap); err != nil {
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
	if ok, err := b.Load(ctx, &snap); err != nil {
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
	snap, err := GetSnapshot(ctx, ss[2], *ref)
	if err != nil {
		return err
	}
	cfg := DefaultConfig(false)
	fsmach := newGotFS(&cfg)
	vcmach := newGotVC(&cfg)
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
	return dst.Apply(ctx, func(dsts [3]stores.RW, x *Snap) (*Snap, error) {
		var g Snap
		var goal *Snap
		if ok, err := src.Load(ctx, &g); err != nil {
			return nil, err
		} else if ok {
			goal = &g
		}

		switch {
		case goal == nil && x == nil:
			return nil, nil
		case goal == nil && !force:
			return nil, fmt.Errorf("cannot clear volume without force=true")
		case x == nil:
		case goal.Equals(*x):
		default:
			vcmach := dst.GotVC()
			hasAncestor, err := vcmach.IsDescendentOf(ctx, dst.VCRO(), *goal, *x)
			if err != nil {
				return nil, err
			}
			if !force && !hasAncestor {
				return nil, fmt.Errorf("cannot CAS, dst ref is not parent of src ref")
			}
		}
		if err := syncStores(ctx, dst.GotVC(), dst.GotFS(), src.RO(), dst.WO(), *goal); err != nil {
			return nil, err
		}
		return goal, nil
	})
}

func History(ctx context.Context, vcmach *VCMach, s stores.Reading, snap Snap, fn func(ref gdat.Ref, snap Snap) error) error {
	ref := vcmach.RefFromSnapshot(snap)
	if err := fn(ref, snap); err != nil {
		return err
	}
	return vcmach.ForEach(ctx, s, snap.Parents, fn)
}

func syncStores(ctx context.Context, vcmach *VCMach, fsmach *gotfs.Machine, src [3]stores.Reading, dst [3]stores.Writing, snap Snap) (err error) {
	ctx, cf := metrics.Child(ctx, "syncing gotvc")
	defer cf()
	return vcmach.Sync(ctx, src[2], dst[2], snap, func(payload Payload) error {
		return fsmach.Sync(ctx, [2]stores.Reading{src[0], src[1]}, [2]stores.Writing{dst[0], dst[1]}, payload.Root)
	})
}

// NewGotFS creates a new gotfs.Machine suitable for writing to the mark
func newGotFS(b *DSConfig, opts ...gotfs.Option) *gotfs.Machine {
	opts = append(opts, gotfs.WithSalt(deriveFSSalt(b)))
	fsag := gotfs.NewMachine(opts...)
	return fsag
}

// NewGotVC creates a new gotvc.Machine suitable for writing to the mark
func newGotVC(b *DSConfig, opts ...gotvc.Option[Payload]) *VCMach {
	opts = append(opts, gotvc.WithSalt[Payload](deriveVCSalt(b)))
	return gotvc.NewMachine(ParsePayload, opts...)
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
