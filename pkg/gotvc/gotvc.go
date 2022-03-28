package gotvc

import (
	"container/list"
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/stores"
)

// ForEach calls fn once for each Ref in the snapshot graph.
func ForEach(ctx context.Context, s cadata.Store, xs []Ref, fn func(Ref, Snapshot) error) error {
	o := NewOperator()
	o.readOnly = true
	visited := map[Ref]struct{}{}
	refs := newRefQueue()
	refs.push(xs...)
	for refs.len() > 0 {
		ref := refs.pop()
		snap, err := o.GetSnapshot(ctx, s, ref)
		if err != nil {
			return err
		}
		if err := fn(ref, *snap); err != nil {
			return err
		}
		visited[ref] = struct{}{}
		for _, parentRef := range snap.Parents {
			if _, exists := visited[parentRef]; !exists {
				refs.push(parentRef)
			}
		}
	}
	return nil
}

type refQueue struct {
	list *list.List
}

func newRefQueue() *refQueue {
	rq := &refQueue{
		list: list.New(),
	}
	return rq
}

func (rq *refQueue) push(xs ...Ref) {
	for _, x := range xs {
		rq.list.PushBack(x)
	}
}

func (rq *refQueue) pop() Ref {
	el := rq.list.Front()
	ret := el.Value.(Ref)
	rq.list.Remove(el)
	return ret
}

func (rq *refQueue) len() int {
	return rq.list.Len()
}

// IsDescendentOf returns true if any of x's parents are equal to a.
func IsDescendentOf(ctx context.Context, s Store, x, a Snapshot) (bool, error) {
	m := map[Ref]struct{}{}
	return isDescendentOf(ctx, m, s, x, a)
}

func isDescendentOf(ctx context.Context, m map[Ref]struct{}, s Store, x, a Snapshot) (bool, error) {
	op := NewOperator()
	op.readOnly = true
	for _, parentRef := range x.Parents {
		if _, exists := m[parentRef]; exists {
			continue
		}
		parent, err := op.GetSnapshot(ctx, s, parentRef)
		if err != nil {
			return false, err
		}
		if parent.Equals(a) {
			return true, nil
		}
		yes, err := IsDescendentOf(ctx, s, *parent, a)
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
func Sync(ctx context.Context, dst, src cadata.Store, snap Snapshot, syncRoot func(gotfs.Root) error) error {
	op := NewOperator()
	op.readOnly = true
	for _, parentRef := range snap.Parents {
		// Skip if the parent is already copieda.
		if exists, err := cadata.Exists(ctx, dst, parentRef.CID); err != nil {
			return err
		} else if !exists {
			parent, err := op.GetSnapshot(ctx, src, parentRef)
			if err != nil {
				return err
			}
			if err := Sync(ctx, dst, src, *parent, syncRoot); err != nil {
				return err
			}
			if err := cadata.Copy(ctx, dst, src, parentRef.CID); err != nil {
				return err
			}
		}
	}
	return syncRoot(snap.Root)
}

// Populate adds all the cadata.IDs reachable from start to set.
// This will not include the CID for start itself, which has not yet been computed.
func Populate(ctx context.Context, s cadata.Store, start Snapshot, set stores.Set, rootFn func(gotfs.Root) error) error {
	for _, parentRef := range start.Parents {
		parentCID := parentRef.CID
		exists, err := set.Exists(ctx, parentCID)
		if err != nil {
			return err
		} else if !exists {
			op := NewOperator()
			parent, err := op.GetSnapshot(ctx, s, parentRef)
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
