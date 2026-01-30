package gotvc

import (
	"context"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
)

type (
	Ref = gdat.Ref
)

// IsDescendentOf returns true if any of x's parents are equal to a.
func (mach *Machine[T]) IsDescendentOf(ctx context.Context, s stores.Reading, x, a Snapshot[T]) (bool, error) {
	m := map[Ref]struct{}{}
	return mach.isDescendentOf(ctx, m, s, x, a)
}

func (mach *Machine[T]) isDescendentOf(ctx context.Context, m map[Ref]struct{}, s stores.Reading, x, a Snapshot[T]) (bool, error) {
	for _, parentRef := range x.Parents {
		if _, exists := m[parentRef]; exists {
			continue
		}
		parent, err := mach.GetSnapshot(ctx, s, parentRef)
		if err != nil {
			return false, err
		}
		if parent.Equals(a) {
			return true, nil
		}
		yes, err := mach.isDescendentOf(ctx, m, s, *parent, a)
		if err != nil {
			return false, err
		}
		if yes {
			return true, nil
		}
		m[parentRef] = struct{}{}
	}
	return false, nil
}

// Sync ensures dst has all of the data reachable from snap.
func (m *Machine[T]) Sync(ctx context.Context, src stores.Reading, dst stores.Writing, snap Snapshot[T], syncp func(T) error) error {
	m2 := *m
	m2.readOnly = true
	var sync func(snap Snapshot[T]) error
	sync = func(snap Snapshot[T]) error {
		for _, parentRef := range snap.Parents {
			// Skip if the parent is already copieda.
			if exists, err := stores.ExistsUnit(ctx, dst, parentRef.CID); err != nil {
				return err
			} else if !exists {
				parent, err := m2.GetSnapshot(ctx, src, parentRef)
				if err != nil {
					return err
				}
				if err := sync(*parent); err != nil {
					return err
				}
				if err := gdat.Copy(ctx, src, dst, &parentRef); err != nil {
					return err
				}
			}
		}
		if err := syncp(snap.Payload); err != nil {
			return err
		}
		metrics.AddInt(ctx, "snapshots", 1, "snapshots")
		return nil
	}
	return sync(snap)
}

// Populate adds all the blobcache.CIDs reachable from start to set.
// This will not include the CID for start itself, which has not yet been computed.
func (mach *Machine[T]) Populate(ctx context.Context, s stores.Reading, start Snapshot[T], set stores.Set, rootFn func(T) error) error {
	for _, parentRef := range start.Parents {
		parentCID := parentRef.CID
		exists, err := set.Exists(ctx, parentCID)
		if err != nil {
			return err
		} else if !exists {
			parent, err := mach.GetSnapshot(ctx, s, parentRef)
			if err != nil {
				return err
			}
			if err := mach.Populate(ctx, s, *parent, set, rootFn); err != nil {
				return err
			}
			if err := set.Add(ctx, parentCID); err != nil {
				return err
			}
		}
	}
	if err := rootFn(start.Payload); err != nil {
		return err
	}
	return nil
}
