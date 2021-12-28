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
	// TODO: take advantage of index copying
	var ent Entry
	for err := it.Next(ctx, &ent); err != kvstreams.EOS; err = it.Next(ctx, &ent) {
		if err != nil {
			return err
		}
		if err := b.Put(ctx, ent.Key, ent.Value); err != nil {
			return err
		}
	}
	return nil
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
	var ents []Entry
	sr := NewStreamReader(s, op, []Index{idx})
	for {
		var ent Entry
		if err := sr.Next(ctx, &ent); err != nil {
			if err == kvstreams.EOS {
				return ents, nil
			}
			return nil, err
		}
		ents = append(ents, ent)
	}
}

func PointsToEntries(root Root) bool {
	return root.Depth == 0
}

func PointsToIndexes(root Root) bool {
	return root.Depth > 0
}
