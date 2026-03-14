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
func (mach *Machine[T]) IsDescendentOf(ctx context.Context, s stores.RO, x, a Vertex[T]) (bool, error) {
	m := map[Ref]struct{}{}
	return mach.isDescendentOf(ctx, m, s, x, a)
}

func (mach *Machine[T]) isDescendentOf(ctx context.Context, m map[Ref]struct{}, s stores.RO, x, a Vertex[T]) (bool, error) {
	for _, parentRef := range x.Parents {
		if _, exists := m[parentRef]; exists {
			continue
		}
		parent, err := mach.GetVertex(ctx, s, parentRef)
		if err != nil {
			return false, err
		}
		if parent.Equals(a) {
			return true, nil
		}
		yes, err := mach.isDescendentOf(ctx, m, s, parent, a)
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

// Sync ensures dst has all of the data reachable from vert.
func (m *Machine[T]) Sync(ctx context.Context, src stores.RO, dst stores.WO, vert Vertex[T], syncp func(T) error) error {
	m2 := *m
	m2.readOnly = true
	var sync func(vert Vertex[T]) error
	sync = func(vert Vertex[T]) error {
		for _, parentRef := range vert.Parents {
			// Skip if the parent is already copieda.
			if exists, err := stores.ExistsUnit(ctx, dst, parentRef.CID); err != nil {
				return err
			} else if !exists {
				parent, err := m2.GetVertex(ctx, src, parentRef)
				if err != nil {
					return err
				}
				if err := sync(parent); err != nil {
					return err
				}
				if err := gdat.Copy(ctx, src, dst, &parentRef); err != nil {
					return err
				}
			}
		}
		if err := syncp(vert.Payload); err != nil {
			return err
		}
		metrics.AddInt(ctx, "commits", 1, "commits")
		return nil
	}
	return sync(vert)
}

// Map traverses the entire history DAG rooted at x, applies fn to each Vertex's Payload,
// and returns a new Vertex with updated parent refs pointing to the transformed parents.
func (mach *Machine[T]) Map(ctx context.Context, s stores.RW, x Vertex[T], fn func(T) (T, error)) (Vertex[T], error) {
	cache := map[Ref]Ref{}
	return mach.mapVertex(ctx, s, x, fn, cache)
}

func (mach *Machine[T]) mapVertex(ctx context.Context, s stores.RW, x Vertex[T], fn func(T) (T, error), cache map[Ref]Ref) (Vertex[T], error) {
	newParents := make([]Ref, len(x.Parents))
	for i, parentRef := range x.Parents {
		if mapped, ok := cache[parentRef]; ok {
			newParents[i] = mapped
			continue
		}
		parent, err := mach.GetVertex(ctx, s, parentRef)
		if err != nil {
			return Vertex[T]{}, err
		}
		mappedParent, err := mach.mapVertex(ctx, s, parent, fn, cache)
		if err != nil {
			return Vertex[T]{}, err
		}
		mappedRef, err := mach.PostVertex(ctx, s, mappedParent)
		if err != nil {
			return Vertex[T]{}, err
		}
		cache[parentRef] = mappedRef
		newParents[i] = mappedRef
	}
	newPayload, err := fn(x.Payload)
	if err != nil {
		return Vertex[T]{}, err
	}
	return Vertex[T]{
		N:         x.N,
		CreatedAt: x.CreatedAt,
		Parents:   newParents,
		Creator:   x.Creator,
		Payload:   newPayload,
	}, nil
}

// Populate adds all the blobcache.CIDs reachable from start to set.
// This will not include the CID for start itself, which has not yet been computed.
func (mach *Machine[T]) Populate(ctx context.Context, s stores.RO, start Vertex[T], set stores.Set, rootFn func(T) error) error {
	for _, parentRef := range start.Parents {
		parentCID := parentRef.CID
		exists, err := set.Exists(ctx, parentCID)
		if err != nil {
			return err
		} else if !exists {
			parent, err := mach.GetVertex(ctx, s, parentRef)
			if err != nil {
				return err
			}
			if err := mach.Populate(ctx, s, parent, set, rootFn); err != nil {
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
