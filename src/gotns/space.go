package gotns

import (
	"context"
	"iter"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/volumes"
	"go.brendoncarroll.net/tai64"
)

var (
	_ gotcore.Space = &Space{}
)

type Space struct {
	Volume volumes.Volume
	DMach  *gdat.Machine
	KVMach *gotkv.Machine
}

func (s *Space) Do(ctx context.Context, modify bool, fn func(sptx gotcore.SpaceTx) error) error {
	tx, err := BeginTx(ctx, s.DMach, s.KVMach, s.Volume, modify)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if err := fn(&SpaceTx{
		kvmach: s.KVMach,
		dmach:  s.DMach,
		tx:     tx,
	}); err != nil {
		return err
	}
	if modify {
		return tx.Commit(ctx)
	}
	return nil
}

var _ gotcore.SpaceTx = &SpaceTx{}

type SpaceTx struct {
	kvmach *gotkv.Machine
	dmach  *gdat.Machine
	tx     *Tx
}

// Create implements gotcore.Space.
func (s *SpaceTx) Create(ctx context.Context, name string, md gotcore.Metadata) (*gotcore.Info, error) {
	prevb, err := s.tx.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if prevb != nil {
		return nil, gotcore.ErrExists
	}
	b := MarkState{
		Info: gotcore.Info{
			Config:      md.Config,
			Annotations: md.Annotations,
			CreatedAt:   tai64.Now().TAI64(),
		},
		// Leave target as zeros for null,
	}
	if err := s.tx.Put(ctx, name, b); err != nil {
		return nil, err
	}
	return &b.Info, nil
}

// Delete implements gotcore.Space.
func (s *SpaceTx) Delete(ctx context.Context, name string) error {
	return s.tx.Delete(ctx, name)
}

// Inspect implements gotcore.Space.
func (s *SpaceTx) Inspect(ctx context.Context, name string) (*gotcore.Info, error) {
	b, err := s.tx.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, gotcore.ErrNotExist
	}
	return &b.Info, nil
}

// List implements gotcore.Space.
func (s *SpaceTx) All(ctx context.Context) iter.Seq2[string, error] {
	return s.tx.AllNames(ctx)
}

// Set implements gotcore.Space.
func (s *SpaceTx) SetMetadata(ctx context.Context, name string, md gotcore.Metadata) error {
	mstate, err := s.tx.Get(ctx, name)
	if err != nil {
		return err
	}
	if mstate == nil {
		return gotcore.ErrNotExist
	}
	mstate.Info.Annotations = md.Annotations
	mstate.Info.Config = md.Config
	return s.tx.Put(ctx, name, *mstate)
}

func (s *SpaceTx) Stores() gotcore.RW {
	return gotcore.RW{
		VC: s.tx.tx,
		FS: gotfs.RW{s.tx.tx, s.tx.tx},
	}
}

func (s *SpaceTx) GetTarget(ctx context.Context, name string) (gdat.Ref, error) {
	mstate, err := s.tx.Get(ctx, name)
	if err != nil {
		return gdat.Ref{}, err
	}
	if mstate == nil {
		return gdat.Ref{}, gotcore.ErrNotExist
	}
	return mstate.Target, nil
}

func (s *SpaceTx) SetTarget(ctx context.Context, name string, ref gdat.Ref) error {
	if ref.CID.IsZero() {
		ref = gdat.Ref{}
	}
	mstate, err := s.tx.Get(ctx, name)
	if err != nil {
		return err
	}
	mstate.Target = ref
	return s.tx.Put(ctx, name, *mstate)
}
