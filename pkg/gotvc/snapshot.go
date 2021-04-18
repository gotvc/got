package gotvc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/pkg/errors"
)

type (
	Store = cadata.Store
	Ref   = gdat.Ref
	Root  = gotfs.Root
)

type Snapshot struct {
	N      uint64     `json:"n"`
	Root   gotfs.Root `json:"root"`
	Parent *gdat.Ref  `json:"parent"`

	Message   string     `json:"message,omitempty"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
}

func NewSnapshot(ctx context.Context, s cadata.Store, root Root, parentRef *gdat.Ref) (*Snapshot, error) {
	if parentRef == nil {
		return &Snapshot{
			N:      0,
			Root:   root,
			Parent: nil,
		}, nil
	}
	parent, err := GetSnapshot(ctx, s, *parentRef)
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		N:      parent.N + 1,
		Root:   root,
		Parent: parentRef,
	}, nil
}

// PostSnapshot marshals the snapshot and posts it to the store
func PostSnapshot(ctx context.Context, s Store, x Snapshot) (*Ref, error) {
	dop := gdat.NewOperator()
	return dop.Post(ctx, s, marshalSnapshot(x))
}

// GetSnapshot retrieves the snapshot referenced by ref from the store.
func GetSnapshot(ctx context.Context, s Store, ref Ref) (*Snapshot, error) {
	dop := gdat.NewOperator()
	var x *Snapshot
	if err := dop.GetF(ctx, s, ref, func(data []byte) error {
		var err error
		x, err = parseCommit(data)
		return err
	}); err != nil {
		return nil, err
	}
	return x, nil
}

// Squash turns multiple snapshots into one.
// It preserves the latest version of the data, but destroys versioning granularity
func Squash(ctx context.Context, s Store, x Snapshot, n int) (*Snapshot, error) {
	if n < 1 {
		return nil, errors.Errorf("cannot squash single commit")
	}
	if x.Parent == nil {
		return nil, errors.Errorf("cannot squash no parent")
	}
	parent, err := GetSnapshot(ctx, s, *x.Parent)
	if err != nil {
		return nil, err
	}
	if n == 1 {
		return &Snapshot{
			N:      parent.N,
			Root:   x.Root,
			Parent: parent.Parent,
		}, nil
	}
	y, err := Squash(ctx, s, *parent, n-1)
	if err != nil {
		return nil, err
	}
	y.Root = x.Root
	return y, nil
}

func Rebase(ctx context.Context, s Store, xs []Snapshot, onto Snapshot) ([]Snapshot, error) {
	var deltas []Delta
	for _, x := range xs {
		var delta Delta
		if x.Parent != nil {
			parent, err := GetSnapshot(ctx, s, *x.Parent)
			if err != nil {
				return nil, err
			}
			d, err := Diff(ctx, s, x, *parent)
			if err != nil {
				return nil, err
			}
			delta = *d
		} else {
			d, err := DiffWithNothing(ctx, s, x)
			if err != nil {
				return nil, err
			}
			delta = *d
		}
		deltas = append(deltas, delta)
	}
	var ys []Snapshot
	for i, delta := range deltas {
		var base Snapshot
		if i == 0 {
			base = onto
		} else {
			base = ys[i-1]
		}
		y, err := ApplyDelta(ctx, s, &base, delta)
		if err != nil {
			return nil, err
		}
		ys = append(ys, *y)
	}
	return ys, nil
}

// HasAncestor returns whether x has a as an ancestor
func HasAncestor(ctx context.Context, s Store, x, a Ref) (bool, error) {
	if gdat.Equal(x, a) {
		return true, nil
	}
	snap, err := GetSnapshot(ctx, s, x)
	if err != nil {
		return false, err
	}
	if snap.Parent == nil {
		return false, nil
	}
	return HasAncestor(ctx, s, *snap.Parent, a)
}

func marshalSnapshot(x Snapshot) []byte {
	data, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return data
}

func parseCommit(data []byte) (*Snapshot, error) {
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

func Copy(ctx context.Context, dst, src cadata.Store, ref gdat.Ref) error {
	snap, err := GetSnapshot(ctx, src, ref)
	if err != nil {
		return err
	}
	if snap.Parent != nil {
		if err := Copy(ctx, dst, src, *snap.Parent); err != nil {
			return err
		}
	}
	if err := gotfs.Copy(ctx, dst, src, snap.Root); err != nil {
		return err
	}
	return cadata.Copy(ctx, dst, src, ref.CID)
}
