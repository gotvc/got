package gotns

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/marks"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
	"go.brendoncarroll.net/tai64"
)

var (
	_ marks.Space = &Space{}
)

type Space struct {
	Volume marks.Volume
	DMach  *gdat.Machine
	KVMach *gotkv.Machine
}

func (s *Space) modify(ctx context.Context, fn func(space *txSpace) error) error {
	tx, err := BeginTx(ctx, s.DMach, s.KVMach, s.Volume, true)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if err := fn(&txSpace{vol: s.Volume, tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Space) view(ctx context.Context, fn func(space *txSpace) error) error {
	tx, err := BeginTx(ctx, s.DMach, s.KVMach, s.Volume, false)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	return fn(&txSpace{vol: s.Volume, tx: tx})
}

// Create implements marks.Space.
func (s *Space) Create(ctx context.Context, name string, cfg marks.Params) (*marks.Info, error) {
	var info *marks.Info
	if err := s.modify(ctx, func(s *txSpace) error {
		info2, err := s.Create(ctx, name, cfg)
		if err != nil {
			return err
		}
		info = info2
		return nil
	}); err != nil {
		return nil, err
	}
	return info, nil
}

// Delete implements marks.Space.
func (s *Space) Delete(ctx context.Context, name string) error {
	return s.modify(ctx, func(space *txSpace) error {
		return space.Delete(ctx, name)
	})
}

// Inspect implements marks.Space.
func (s *Space) Inspect(ctx context.Context, name string) (*marks.Info, error) {
	var info *marks.Info
	if err := s.view(ctx, func(space *txSpace) error {
		info2, err := space.Inspect(ctx, name)
		if err != nil {
			return err
		}
		info = info2
		return nil
	}); err != nil {
		return nil, err
	}
	return info, nil
}

// List implements marks.Space.
func (s *Space) List(ctx context.Context, span marks.Span, limit int) ([]string, error) {
	var names []string
	if err := s.view(ctx, func(space *txSpace) error {
		names2, err := space.List(ctx, span, limit)
		if err != nil {
			return err
		}
		names = names2
		return nil
	}); err != nil {
		return nil, err
	}
	return names, nil
}

// Open implements marks.Space.
func (s *Space) Open(ctx context.Context, name string) (*marks.Mark, error) {
	info, err := s.Inspect(ctx, name)
	if err != nil {
		return nil, err
	}
	vol := &VirtVolume{
		vol:     s.Volume,
		kvmach:  s.KVMach,
		dmach:   s.DMach,
		name:    name,
		closeTx: true,
	}
	return &marks.Mark{
		Volume: vol,
		Info:   *info,
	}, nil
}

// Set implements marks.Space.
func (s *Space) Set(ctx context.Context, name string, cfg marks.Params) error {
	return s.modify(ctx, func(s *txSpace) error {
		return s.Set(ctx, name, cfg)
	})
}

type txSpace struct {
	vol    marks.Volume
	kvmach *gotkv.Machine
	dmach  *gdat.Machine
	tx     *Tx
}

// Create implements marks.Space.
func (s *txSpace) Create(ctx context.Context, name string, cfg marks.Params) (*marks.Info, error) {
	prevb, err := s.tx.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if prevb != nil {
		return nil, marks.ErrExists
	}
	emptyRef, err := s.tx.dmach.Post(ctx, s.tx.tx, nil)
	if err != nil {
		return nil, err
	}
	b := MarkState{
		Info: marks.Info{
			Salt:        cfg.Salt,
			Annotations: cfg.Annotations,
			CreatedAt:   tai64.Now().TAI64(),
		},
		Target: *emptyRef,
	}
	if err := s.tx.Put(ctx, name, b); err != nil {
		return nil, err
	}
	return &b.Info, nil
}

// Delete implements marks.Space.
func (s *txSpace) Delete(ctx context.Context, name string) error {
	return s.tx.Delete(ctx, name)
}

// Inspect implements marks.Space.
func (s *txSpace) Inspect(ctx context.Context, name string) (*marks.Info, error) {
	b, err := s.tx.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, marks.ErrNotExist
	}
	return &b.Info, nil
}

// List implements marks.Space.
func (s *txSpace) List(ctx context.Context, span marks.Span, limit int) ([]string, error) {
	return s.tx.ListNames(ctx, span, limit)
}

// Set implements marks.Space.
func (s *txSpace) Set(ctx context.Context, name string, cfg marks.Params) error {
	panic("unimplemented")
}

var _ volumes.Volume = &VirtVolume{}

// VirtVolume is a virtual volume containing the a Marks root as the VirtVolume root.
type VirtVolume struct {
	vol     marks.Volume
	kvmach  *gotkv.Machine
	dmach   *gdat.Machine
	name    string
	closeTx bool
}

var _ volumes.Tx = &VirtVolumeTx{}

// BeginTx implements volumes.Volume.
func (v *VirtVolume) BeginTx(ctx context.Context, tp volumes.TxParams) (volumes.Tx, error) {
	tx, err := v.vol.BeginTx(ctx, tp)
	if err != nil {
		return nil, err
	}
	tx2 := NewTx(tx, v.dmach, v.kvmach)
	return &VirtVolumeTx{tx: tx2, name: v.name, closeTx: v.closeTx}, nil
}

// VirtVolumeTx is a transaction on a branch volume.
type VirtVolumeTx struct {
	tx   *Tx
	name string
	// closeTx causes Commit and Abort to also be called on tx.
	closeTx bool

	root []byte
}

// Abort implements volumes.Tx.
func (vvt *VirtVolumeTx) Abort(ctx context.Context) error {
	if vvt.closeTx {
		if err := vvt.tx.Abort(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Commit implements volumes.Tx.
func (vvt *VirtVolumeTx) Commit(ctx context.Context) error {
	if vvt.root != nil {
		if err := vvt.tx.SaveMarkRoot(ctx, vvt.name, vvt.root); err != nil {
			return err
		}
	}
	if vvt.closeTx {
		if err := vvt.tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Exists implements volumes.Tx.
func (vvt *VirtVolumeTx) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return vvt.tx.tx.Exists(ctx, cids, dst)
}

// Get implements volumes.Tx.
func (vvt *VirtVolumeTx) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return vvt.tx.tx.Get(ctx, cid, buf)
}

// Post implements volumes.Tx.
func (vvt *VirtVolumeTx) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	return vvt.tx.tx.Post(ctx, data)
}

// Load implements volumes.Tx.
func (vvt *VirtVolumeTx) Load(ctx context.Context, dst *[]byte) error {
	return vvt.tx.LoadBranchRoot(ctx, vvt.name, dst)
}

// Save implements volumes.Tx.
func (vvt *VirtVolumeTx) Save(ctx context.Context, src []byte) error {
	vvt.root = append(vvt.root[:0], src...)
	return nil
}

// Hash implements volumes.Tx.
func (vvt *VirtVolumeTx) Hash(data []byte) blobcache.CID {
	return stores.Hash(data)
}

// MaxSize implements volumes.Tx.
func (vvt *VirtVolumeTx) MaxSize() int {
	return vvt.tx.tx.MaxSize()
}
