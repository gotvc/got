package ptree

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/pkg/errors"
)

const maxTreeDepth = 255

// Root is the root of the tree
type Root struct {
	Ref   Ref    `json:"ref"`
	Depth uint8  `json:"depth"`
	First []byte `json:"first,omitempty"`
}

// Copy copies all the entries from it to b.
func Copy(ctx context.Context, b *Builder, it *Iterator) error {
	var ent Entry
	for {
		level := min(b.syncLevel(), it.syncLevel())
		if err := it.next(ctx, level, &ent); err != nil {
			if err == kvstreams.EOS {
				return nil
			}
			return err
		}
		if err := b.put(ctx, level, ent.Key, ent.Value); err != nil {
			return err
		}
	}
}

// ListChildren returns the immediate children of root if any.
func ListChildren(ctx context.Context, s cadata.Store, op *gdat.Operator, root Root) ([]Index, error) {
	if PointsToEntries(root) {
		return nil, errors.Errorf("cannot list children of root with depth=%d", root.Depth)
	}
	sr := NewStreamReader(s, op, []Index{rootToIndex(root)})
	var idxs []Index
	var ent Entry
	for {
		if err := sr.Next(ctx, &ent); err != nil {
			if err == kvstreams.EOS {
				break
			}
			return nil, err
		}
		idx, err := entryToIndex(ent)
		if err != nil {
			return nil, err
		}
		idxs = append(idxs, idx)
	}
	return idxs, nil
}

// ListEntries
func ListEntries(ctx context.Context, s cadata.Store, op *gdat.Operator, idx Index) ([]Entry, error) {
	sr := NewStreamReader(s, op, []Index{idx})
	return kvstreams.Collect(ctx, sr)
}

func PointsToEntries(root Root) bool {
	return root.Depth == 0
}

func PointsToIndexes(root Root) bool {
	return root.Depth > 0
}

func entryToIndex(ent Entry) (Index, error) {
	ref, err := gdat.ParseRef(ent.Value)
	if err != nil {
		return Index{}, err
	}
	return Index{
		First: append([]byte{}, ent.Key...),
		Ref:   *ref,
	}, nil
}

func indexToEntry(idx Index) Entry {
	return Entry{Key: idx.First, Value: gdat.MarshalRef(idx.Ref)}
}

func indexToRoot(idx Index, depth uint8) Root {
	return Root{
		Ref:   idx.Ref,
		First: idx.First,
		Depth: depth,
	}
}

func rootToIndex(r Root) Index {
	return Index{
		Ref:   r.Ref,
		First: r.First,
	}
}
