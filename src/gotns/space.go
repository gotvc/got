package gotns

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
	"go.brendoncarroll.net/tai64"
)

var (
	_ branches.Space = &TxSpace{}
	_ branches.Space = &Space{}
)

type Space struct {
	Volume branches.Volume
	DMach  *gdat.Machine
	KVMach *gotkv.Machine
}

func (s *Space) modify(ctx context.Context, fn func(space *TxSpace) error) error {
	tx, err := BeginTx(ctx, s.DMach, s.KVMach, s.Volume, true)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if err := fn(&TxSpace{Tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Space) view(ctx context.Context, fn func(space *TxSpace) error) error {
	tx, err := BeginTx(ctx, s.DMach, s.KVMach, s.Volume, false)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	return fn(&TxSpace{Tx: tx})
}

// Create implements branches.Space.
func (s *Space) Create(ctx context.Context, name string, cfg branches.Params) (*branches.Info, error) {
	var info *branches.Info
	if err := s.modify(ctx, func(s *TxSpace) error {
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

// Delete implements branches.Space.
func (s *Space) Delete(ctx context.Context, name string) error {
	return s.modify(ctx, func(space *TxSpace) error {
		return space.Delete(ctx, name)
	})
}

// Inspect implements branches.Space.
func (s *Space) Inspect(ctx context.Context, name string) (*branches.Info, error) {
	var info *branches.Info
	if err := s.view(ctx, func(space *TxSpace) error {
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

// List implements branches.Space.
func (s *Space) List(ctx context.Context, span branches.Span, limit int) ([]string, error) {
	var names []string
	if err := s.view(ctx, func(space *TxSpace) error {
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

// Open implements branches.Space.
func (s *Space) Open(ctx context.Context, name string) (*branches.Branch, error) {
	info, err := s.Inspect(ctx, name)
	if err != nil {
		return nil, err
	}
	tx, err := BeginTx(ctx, s.DMach, s.KVMach, s.Volume, true)
	if err != nil {
		return nil, err
	}
	vol := &VirtVolume{
		tx:      tx,
		name:    name,
		closeTx: true,
	}
	return &branches.Branch{
		Volume: vol,
		Info:   *info,
	}, nil
}

// Set implements branches.Space.
func (s *Space) Set(ctx context.Context, name string, cfg branches.Params) error {
	return s.modify(ctx, func(s *TxSpace) error {
		return s.Set(ctx, name, cfg)
	})
}

type TxSpace struct {
	Tx *Tx
}

// Create implements branches.Space.
func (s *TxSpace) Create(ctx context.Context, name string, cfg branches.Params) (*branches.Info, error) {
	prevb, err := s.Tx.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if prevb != nil {
		return nil, branches.ErrExists
	}
	emptyRef, err := s.Tx.dmach.Post(ctx, s.Tx.tx, nil)
	if err != nil {
		return nil, err
	}
	b := MarkState{
		Info: branches.Info{
			Salt:        cfg.Salt,
			Annotations: cfg.Annotations,
			CreatedAt:   tai64.Now().TAI64(),
		},
		Target: *emptyRef,
	}
	if err := s.Tx.Put(ctx, name, b); err != nil {
		return nil, err
	}
	return &b.Info, nil
}

// Delete implements branches.Space.
func (s *TxSpace) Delete(ctx context.Context, name string) error {
	return s.Tx.Delete(ctx, name)
}

// Inspect implements branches.Space.
func (s *TxSpace) Inspect(ctx context.Context, name string) (*branches.Info, error) {
	b, err := s.Tx.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, branches.ErrNotExist
	}
	return &b.Info, nil
}

// List implements branches.Space.
func (s *TxSpace) List(ctx context.Context, span branches.Span, limit int) ([]string, error) {
	return s.Tx.ListNames(ctx, span, limit)
}

// Open implements branches.Space.
func (s *TxSpace) Open(ctx context.Context, name string) (*branches.Branch, error) {
	b, err := s.Tx.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, branches.ErrNotExist
	}
	vol := &VirtVolume{
		tx:      s.Tx,
		name:    name,
		closeTx: false,
	}
	return &branches.Branch{
		Volume: vol,
		Info:   b.Info,
	}, nil
}

// Set implements branches.Space.
func (s *TxSpace) Set(ctx context.Context, name string, cfg branches.Params) error {
	panic("unimplemented")
}

var _ volumes.Volume = &VirtVolume{}

// VirtVolume is a virtual volume containing the a Marks root as the VirtVolume root.
type VirtVolume struct {
	tx      *Tx
	name    string
	closeTx bool
}

var _ volumes.Tx = &VirtVolumeTx{}

// BeginTx implements volumes.Volume.
func (v *VirtVolume) BeginTx(ctx context.Context, tp volumes.TxParams) (volumes.Tx, error) {
	return &VirtVolumeTx{tx: v.tx, name: v.name, closeTx: v.closeTx}, nil
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
