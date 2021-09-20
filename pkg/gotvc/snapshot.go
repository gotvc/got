package gotvc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type (
	Store = cadata.Store
	Ref   = gdat.Ref
	Root  = gotfs.Root
	Snap  = Snapshot
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

func Change(ctx context.Context, s cadata.Store, base *Snapshot, fn func(root *gotfs.Root) (*gotfs.Root, error)) (*Snapshot, error) {
	var parentRef *Ref
	var n uint64
	var root *Root
	if base != nil {
		var err error
		parentRef, err = PostSnapshot(ctx, s, *base)
		if err != nil {
			return nil, err
		}
		n = base.N + 1
		root = &base.Root
	}
	root, err := fn(root)
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		Parent: parentRef,
		N:      n,
		Root:   *root,
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
		x, err = parseSnapshot(data)
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

func parseSnapshot(data []byte) (*Snapshot, error) {
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

func RefFromSnapshot(snap Snapshot) Ref {
	ref, err := PostSnapshot(context.Background(), cadata.Void{}, snap)
	if err != nil {
		panic(err)
	}
	return *ref
}

// Sync ensures dst has all of the data reachable from snap.
func Sync(ctx context.Context, dst, src cadata.Store, snap Snapshot, syncRoot func(gotfs.Root) error) error {
	if snap.Parent != nil {
		// Skip if the parent is already copied.
		if exists, err := dst.Exists(ctx, snap.Parent.CID); err != nil {
			return err
		} else if !exists {
			parent, err := GetSnapshot(ctx, src, *snap.Parent)
			if err != nil {
				return err
			}
			if err := Sync(ctx, dst, src, *parent, syncRoot); err != nil {
				return err
			}
			if err := cadata.Copy(ctx, dst, src, snap.Parent.CID); err != nil {
				return err
			}
		}
	}
	return syncRoot(snap.Root)
}

// Check ensures that snapshot is valid.
func Check(ctx context.Context, s cadata.Store, snap Snapshot, checkRoot func(gotfs.Root) error) error {
	logrus.Infof("checking commit #%d", snap.N)
	if err := checkRoot(snap.Root); err != nil {
		return err
	}
	if snap.Parent == nil {
		return nil
	}
	parent, err := GetSnapshot(ctx, s, *snap.Parent)
	if err != nil {
		return err
	}
	return Check(ctx, s, *parent, checkRoot)
}

// ForEachAncestor call fn once for each ancestor of snap, and snap in reverse order.
func ForEachAncestor(ctx context.Context, s cadata.Store, snap Snapshot, fn func(Ref, Snapshot) error) error {
	ref, err := PostSnapshot(ctx, cadata.Void{}, snap)
	if err != nil {
		return err
	}
	for {
		if err := fn(*ref, snap); err != nil {
			return err
		}
		if snap.Parent == nil {
			return nil
		}
		next, err := GetSnapshot(ctx, s, *snap.Parent)
		if err != nil {
			return err
		}
		ref = snap.Parent
		snap = *next
	}
}
