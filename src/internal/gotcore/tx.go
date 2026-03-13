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

func (mtx *MarkTx) FSRO() gotfs.RO {
	return mtx.RO().FS
}

func (mtx *MarkTx) FSRW() gotfs.RW {
	ss := mtx.stx.Stores()
	return ss.FS
}

func (mtx *MarkTx) VCRO() stores.RO {
	ss := mtx.RO()
	return ss.VC
}

func (mtx *MarkTx) VCRW() stores.RW {
	ss := mtx.stx.Stores()
	return ss.VC
}

func (mtx *MarkTx) RO() RO {
	return mtx.stx.Stores().RO()
}

func (mtx *MarkTx) WO() WO {
	return mtx.stx.Stores().WO()
}

// Save saves the commit to the Mark
func (m *MarkTx) Save(ctx context.Context, ref gdat.Ref) error {
	ss := m.stx.Stores()
	if !ref.IsZero() {
		if exists, err := stores.ExistsUnit(ctx, ss.VC, ref.CID); err != nil {
			return err
		} else if !exists {
			return ErrRefIntegrity{Ref: ref, Store: "gotvc"}
		}
	}
	return m.stx.SetTarget(ctx, m.name, ref)
}

// Load loads a commit from the Mark
func (b *MarkTx) Load(ctx context.Context, dst *gdat.Ref) (bool, error) {
	return b.stx.GetTarget(ctx, b.name, dst)
}

func (mt *MarkTx) LoadCommit(ctx context.Context, dst *Commit) (bool, error) {
	var ref gdat.Ref
	if ok, err := mt.Load(ctx, &ref); err != nil {
		return false, err
	} else if ok {
		comm, err := mt.GotVC().GetVertex(ctx, mt.VCRO(), ref)
		if err != nil {
			return false, err
		}
		*dst = *comm
		return true, nil
	} else {
		return false, nil
	}
}

// Apply calls fn with the Marks target Ref
// If the mark does not yet have a target, then the ref will be 0.
func (m *MarkTx) Apply(ctx context.Context, fn func(RW, gdat.Ref) (gdat.Ref, error)) error {
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

func (m *MarkTx) Modify(ctx context.Context, fn func(mctx ModifyCtx) (*Commit, error)) error {
	m.init()
	ss := m.stx.Stores()
	var comm *Commit
	var target gdat.Ref
	if ok, err := m.stx.GetTarget(ctx, m.name, &target); err != nil {
		return err
	} else if ok {
		comm, err = m.GotVC().GetVertex(ctx, ss.VC, target)
		if err != nil {
			return err
		}
	}
	modctx := ModifyCtx{
		VC:     m.GotVC(),
		FS:     m.GotFS(),
		Stores: ss,
		Root:   comm,
	}
	y, err := fn(modctx)
	if err != nil {
		return err
	}
	var yRef gdat.Ref
	if y != nil {
		ref, err := m.GotVC().PostVertex(ctx, m.VCRW(), *y)
		if err != nil {
			return err
		}
		ref, err = syncCommitRef(ctx, m.gotvc, m.gotfs, m.RO(), m.WO(), ref)
		if err != nil {
			return err
		}
		yRef = ref
	}
	return m.Save(ctx, yRef)
}

// ModifyCtx is the context passed to the modify function.
type ModifyCtx struct {
	VC     *VCMach
	FS     *FSMach
	Stores RW
	Root   *Commit
}

// Sync syncs a commit into the store
func (mctx *ModifyCtx) Sync(ctx context.Context, srcs RO, root Commit) error {
	return mctx.VC.Sync(ctx, srcs.VC, mctx.Stores.VC, root, func(payload Payload) error {
		return mctx.FS.Sync(ctx,
			srcs.FS,
			mctx.Stores.FS.WO(),
			payload.Snap,
		)
	})
}

func (b *MarkTx) History(ctx context.Context, fn func(ref gdat.Ref, comm Commit) error) error {
	b.init()
	var comm Commit
	if ok, err := b.LoadCommit(ctx, &comm); err != nil {
		return err
	} else if !ok {
		return nil
	}
	ref := b.gotvc.RefFromVertex(comm)
	if err := fn(ref, comm); err != nil {
		return err
	}
	return b.gotvc.ForEach(ctx, b.VCRO(), comm.Parents, fn)
}

func (b *MarkTx) LoadFS(ctx context.Context, dst *gotfs.Root) (bool, error) {
	b.init()
	var comm Commit
	if ok, err := b.LoadCommit(ctx, &comm); err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}
	*dst = comm.Payload.Snap
	return true, nil
}

type ViewCtx struct {
	VC     *VCMach
	FS     *FSMach
	Stores RO
	Target gdat.Ref
	Root   *Commit
}

func (vc *ViewCtx) FSRO() gotfs.RO {
	return vc.Stores.FS
}

// ViewCommit calls fn with everything needed to read a Commitshot, its filesystem, and its ancestry.
func ViewCommit(ctx context.Context, stx SpaceTx, se CommitExpr, fn func(vctx *ViewCtx) error) error {
	ref, err := se.Resolve(ctx, stx)
	if err != nil {
		return err
	}
	ss := stx.Stores()
	cfg := DefaultConfig(false)
	fsmach := newGotFS(&cfg)
	vcmach := newGotVC(&cfg)
	comm, err := vcmach.GetVertex(ctx, ss.VC, *ref)
	if err != nil {
		return err
	}
	vctx := ViewCtx{
		VC:     vcmach,
		FS:     fsmach,
		Stores: ss.RO(),
		Target: *ref,
		Root:   comm,
	}
	return fn(&vctx)
}

// SyncVolumes syncs the contents of src to dst.
func Sync(ctx context.Context, src, dst *MarkTx, force bool) error {
	if !force && src.info.Config.Salt != dst.info.Config.Salt {
		return fmt.Errorf("cannot sync volumes with different salts, must use force=true")
	}
	return dst.Apply(ctx, func(dsts RW, x gdat.Ref) (gdat.Ref, error) {
		var goal *Commit
		var goalRef gdat.Ref
		if ok, err := src.Load(ctx, &goalRef); err != nil {
			return gdat.Ref{}, err
		} else if ok {
			var err error
			goal, err = src.GotVC().GetVertex(ctx, src.VCRO(), goalRef)
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
			prevCommit, err := vcmach.GetVertex(ctx, dst.VCRO(), x)
			if err != nil {
				return gdat.Ref{}, err
			}
			hasAncestor, err := vcmach.IsDescendentOf(ctx, src.VCRO(), *goal, *prevCommit)
			if err != nil {
				return gdat.Ref{}, err
			}
			if !force && !hasAncestor {
				return gdat.Ref{}, fmt.Errorf("cannot CAS, prev ref %v is not parent of next ref %v", x, goalRef)
			}
		}
		ref, err := syncCommitRef(ctx, dst.GotVC(), dst.GotFS(), src.RO(), dst.WO(), goalRef)
		if err != nil {
			return gdat.Ref{}, err
		}
		return ref, nil
	})
}

func History(ctx context.Context, vcmach *VCMach, s stores.RO, commRef gdat.Ref, fn func(ref gdat.Ref, comm Commit) error) error {
	comm, err := vcmach.GetVertex(ctx, s, commRef)
	if err != nil {
		return err
	}
	if err := fn(commRef, *comm); err != nil {
		return err
	}
	return vcmach.ForEach(ctx, s, comm.Parents, fn)
}

// syncCommitRef ensures that all content reachable from Ref is in the dst store.
// blobs are copied from the source store as needed.
func syncCommitRef(ctx context.Context, vcmach *VCMach, fsmach *gotfs.Machine, src RO, dst WO, ref gdat.Ref) (_ gdat.Ref, err error) {
	ctx, cf := metrics.Child(ctx, "syncing gotvc")
	defer cf()
	comm, err := vcmach.GetVertex(ctx, src.VC, ref)
	if err != nil {
		return gdat.Ref{}, err
	}
	if err := vcmach.Sync(ctx, src.VC, dst.VC, *comm, func(payload Payload) error {
		return fsmach.Sync(ctx, src.FS, dst.FS, payload.Snap)
	}); err != nil {
		return gdat.Ref{}, err
	}
	return vcmach.PostVertex(ctx, dst.VC, *comm)
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
