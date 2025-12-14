package gotns

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/volumes"
	"go.brendoncarroll.net/tai64"
)

var _ branches.Space = &Space{}

type Space struct {
	Blobcache blobcache.Service
	Tx        *Tx
}

// Create implements branches.Space.
func (s *Space) Create(ctx context.Context, name string, cfg branches.Params) (*branches.Info, error) {
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
	b := BranchState{
		Info: branches.Info{
			Salt:        cfg.Salt,
			Annotations: cfg.Annotations,
			CreatedAt:   tai64.Now().TAI64(),
		},
		To: *emptyRef,
	}
	if err := s.Tx.Put(ctx, name, b); err != nil {
		return nil, err
	}
	return &b.Info, nil
}

// Delete implements branches.Space.
func (s *Space) Delete(ctx context.Context, name string) error {
	return s.Tx.Delete(ctx, name)
}

// Inspect implements branches.Space.
func (s *Space) Inspect(ctx context.Context, name string) (*branches.Info, error) {
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
func (s *Space) List(ctx context.Context, span branches.Span, limit int) ([]string, error) {
	return s.Tx.ListNames(ctx, span, limit)
}

// Open implements branches.Space.
func (s *Space) Open(ctx context.Context, name string) (*branches.Branch, error) {
	b, err := s.Tx.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, branches.ErrNotExist
	}
	vol, err := s.openVol(name)
	if err != nil {
		return nil, err
	}
	return &branches.Branch{
		Volume: vol,
		Info:   b.Info,
	}, nil
}

func (s *Space) openVol(name string) (volumes.Volume, error) {
	return &Volume{
		tx:   s.Tx,
		name: name,
	}, nil
}

// Set implements branches.Space.
func (s *Space) Set(ctx context.Context, name string, cfg branches.Params) error {
	panic("unimplemented")
}

var _ volumes.Volume = &Volume{}

type Volume struct {
	tx   *Tx
	name string
}

var _ volumes.Tx = &BranchTx{}

// BeginTx implements volumes.Volume.
func (v *Volume) BeginTx(ctx context.Context, tp volumes.TxParams) (volumes.Tx, error) {
	return &BranchTx{tx: v.tx, name: v.name}, nil
}

// BranchTx is a transaction on a branch volume.
type BranchTx struct {
	tx   *Tx
	name string
	// closeTx causes Commit and Abort to also be called on tx.
	closeTx bool

	root []byte
}

// Abort implements volumes.Tx.
func (b *BranchTx) Abort(ctx context.Context) error {
	if b.closeTx {
		if err := b.tx.Abort(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Commit implements volumes.Tx.
func (b *BranchTx) Commit(ctx context.Context) error {
	if b.root != nil {
		if err := b.tx.SaveBranchRoot(ctx, b.name, b.root); err != nil {
			return err
		}
	}
	if b.closeTx {
		if err := b.tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Exists implements volumes.Tx.
func (b *BranchTx) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return b.tx.tx.Exists(ctx, cids, dst)
}

// Get implements volumes.Tx.
func (b *BranchTx) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return b.tx.tx.Get(ctx, cid, buf)
}

// Post implements volumes.Tx.
func (b *BranchTx) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	return b.tx.tx.Post(ctx, data)
}

// Load implements volumes.Tx.
func (b *BranchTx) Load(ctx context.Context, dst *[]byte) error {
	return b.tx.LoadBranchRoot(ctx, b.name, dst)
}

// Save implements volumes.Tx.
func (b *BranchTx) Save(ctx context.Context, src []byte) error {
	b.root = append(b.root[:0], src...)
	return nil
}

// Hash implements volumes.Tx.
func (b *BranchTx) Hash(data []byte) blobcache.CID {
	return stores.Hash(data)
}

// MaxSize implements volumes.Tx.
func (b *BranchTx) MaxSize() int {
	return b.tx.tx.MaxSize()
}
