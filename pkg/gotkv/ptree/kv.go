package ptree

import (
	"bytes"
	"context"

	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

type (
	Span  = kvstreams.Span
	Entry = kvstreams.Entry
)

// MaxEntry returns the entry in span with the greatest (ordered last) key.
func MaxEntry(ctx context.Context, cmp CompareFunc, s Getter, x Root, span Span) (*Entry, error) {
	sr := NewStreamReader(StreamReaderParams{
		Store:   s,
		Compare: cmp,
		Indexes: []Index{rootToIndex(x)},
	})
	ent, err := maxEntry(ctx, sr, span.End)
	if err != nil {
		return nil, err
	}
	if ent == nil {
		return nil, nil
	}
	if x.Depth == 0 {
		if span.AllGt(ent.Key) {
			return nil, nil
		}
		return ent, nil
	}
	idx, err := entryToIndex(*ent)
	if err != nil {
		return nil, err
	}
	return MaxEntry(ctx, cmp, s, indexToRoot(idx, x.Depth-1), span)
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
