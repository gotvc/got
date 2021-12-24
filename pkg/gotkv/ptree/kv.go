package ptree

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/pkg/errors"
)

type (
	Span  = kvstreams.Span
	Entry = kvstreams.Entry
)

// MaxEntry returns the entry in span with the greatest (ordered last) key.
func MaxEntry(ctx context.Context, s cadata.Store, x Root, span Span) (*Entry, error) {
	op := gdat.NewOperator()
	sr := NewStreamReader(s, &op, []Index{rootToIndex(x)})
	ent, err := maxEntry(ctx, sr, span.End)
	if err != nil {
		return nil, err
	}
	if ent == nil {
		return nil, nil
	}
	if x.Depth == 0 {
		if span.GreaterThan(ent.Key) {
			return nil, nil
		}
		return ent, nil
	}
	idx, err := entryToIndex(*ent)
	if err != nil {
		return nil, err
	}
	return MaxEntry(ctx, s, indexToRoot(idx, x.Depth-1), span)
}

func maxEntry(ctx context.Context, sr *StreamReader, under []byte) (ret *Entry, _ error) {
	// TODO: this can be more efficient using Peek
	var ent Entry
	for err := sr.Next(ctx, &ent); err != kvstreams.EOS; err = sr.Next(ctx, &ent) {
		if err != nil {
			return nil, err
		}
		if under != nil && bytes.Compare(ent.Key, under) >= 0 {
			break
		}
		ent2 := ent.Clone()
		ret = &ent2
	}
	return ret, nil
}

// AddPrefix returns a new version of root with the prefix prepended to all the keys
func AddPrefix(x Root, prefix []byte) Root {
	var first []byte
	first = append(first, prefix...)
	first = append(first, x.First...)
	y := Root{
		First: first,
		Ref:   x.Ref,
		Depth: x.Depth,
	}
	return y
}

// RemovePrefix returns a new version of root with the prefix removed from all the keys
func RemovePrefix(ctx context.Context, s cadata.Store, x Root, prefix []byte) (*Root, error) {
	if yes, err := HasPrefix(ctx, s, x, prefix); err != nil {
		return nil, err
	} else if yes {
		return nil, errors.Errorf("tree does not have prefix %q", prefix)
	}
	y := Root{
		First: append([]byte{}, x.First[len(prefix):]...),
		Ref:   x.Ref,
		Depth: x.Depth,
	}
	return &y, nil
}

// HasPrefix returns true if the tree rooted at x only has keys which are prefixed with prefix
func HasPrefix(ctx context.Context, s cadata.Store, x Root, prefix []byte) (bool, error) {
	if !bytes.HasPrefix(x.First, prefix) {
		return false, nil
	}
	maxEnt, err := MaxEntry(ctx, s, x, kvstreams.TotalSpan())
	if err != nil {
		return false, err
	}
	if !bytes.HasPrefix(maxEnt.Key, prefix) {
		return false, nil
	}
	return true, nil
}
