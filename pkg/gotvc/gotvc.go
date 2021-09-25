package gotvc

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/stores"
)

// ForEach calls fn once for each ancestor of the snap at ref, and the snap at ref itself
func ForEach(ctx context.Context, s cadata.Store, x Ref, fn func(Ref, Snapshot) error) error {
	o := NewOperator()
	o.readOnly = true
	for ref := &x; ref != nil; {
		snap, err := o.GetSnapshot(ctx, s, *ref)
		if err != nil {
			return err
		}
		if err := fn(*ref, *snap); err != nil {
			return err
		}
		ref = snap.Parent
	}
	return nil
}

// IsDescendentOf returns true if any of x's parents are equal to a.
func IsDescendentOf(ctx context.Context, s Store, x, a Snapshot) (bool, error) {
	op := NewOperator()
	op.readOnly = true
	if x.Parent == nil {
		return false, nil
	}
	parent, err := op.GetSnapshot(ctx, s, *x.Parent)
	if err != nil {
		return false, err
	}
	if parent.Equals(a) {
		return true, nil
	}
	return IsDescendentOf(ctx, s, *parent, a)
}

// HasAncestor returns whether x has a as an ancestor.
// As a special case, x is considered to be it's own ancestor
func HasAncestor(ctx context.Context, s Store, x, a Snapshot) (bool, error) {
	op := NewOperator()
	op.readOnly = true
	if x.Equals(a) {
		return true, nil
	}
	if x.Parent == nil {
		return false, nil
	}
	snap, err := op.GetSnapshot(ctx, s, *x.Parent)
	if err != nil {
		return false, err
	}
	return HasAncestor(ctx, s, *snap, a)
}

// Sync ensures dst has all of the data reachable from snap.
func Sync(ctx context.Context, dst, src cadata.Store, snap Snapshot, syncRoot func(gotfs.Root) error) error {
	op := NewOperator()
	op.readOnly = true
	if snap.Parent != nil {
		// Skip if the parent is already copied.
		if exists, err := dst.Exists(ctx, snap.Parent.CID); err != nil {
			return err
		} else if !exists {
			parent, err := op.GetSnapshot(ctx, src, *snap.Parent)
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

// Populate adds all the cadata.IDs reachable from start to set.
// This will not include the CID for start itself, which has not yet been computed.
func Populate(ctx context.Context, s cadata.Store, start Snapshot, set stores.Set, rootFn func(gotfs.Root) error) error {
	if start.Parent != nil {
		parentCID := start.Parent.CID
		exists, err := set.Exists(ctx, parentCID)
		if err != nil {
			return err
		} else if !exists {
			op := NewOperator()
			parent, err := op.GetSnapshot(ctx, s, *start.Parent)
			if err != nil {
				return err
			}
			if err := Populate(ctx, s, *parent, set, rootFn); err != nil {
				return err
			}
			if err := set.Add(ctx, parentCID); err != nil {
				return err
			}
		}
	}
	if err := rootFn(start.Root); err != nil {
		return err
	}
	return nil
}
