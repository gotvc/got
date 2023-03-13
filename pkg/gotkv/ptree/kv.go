package ptree

import (
	"context"
	"errors"

	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

// MaxEntry returns the entry in span with the greatest (ordered last) key.
func MaxEntry[T, Ref any](ctx context.Context, params ReadParams[T, Ref], x Root[T, Ref], lt T) (*T, error) {
	sr := NewStreamReader(StreamReaderParams[T, Ref]{
		Store:   params.Store,
		Compare: params.Compare,
		Decoder: params.NewDecoder(),
		Indexes: []Index[T, Ref]{rootToIndex(x)},
	})
	ent, err := maxEntry(ctx, sr, params.Compare, &lt)
	if err != nil {
		return nil, err
	}
	if ent == nil {
		return nil, nil
	}
	if x.Depth == 0 {
		if params.Compare(lt, *ent) > 0 {
			return nil, nil
		}
		return ent, nil
	}
	idx, err := params.ConvertEntry(*ent)
	if err != nil {
		return nil, err
	}
	return MaxEntry(ctx, params, indexToRoot(idx, x.Depth-1), lt)
}

func maxEntry[T, Ref any](ctx context.Context, sr *StreamReader[T, Ref], cmp func(a, b T) int, under *T) (ret *T, _ error) {
	// TODO: this can be more efficient using Peek
	var ent T
	var found bool
	for {
		if err := sr.Peek(ctx, &ent); err != nil {
			if errors.Is(err, kvstreams.EOS) {
				break
			}
			return nil, err
		}
		if under != nil && cmp(ent, *under) >= 0 {
			break // not under
		}
		if err := sr.Next(ctx, &ent); err != nil {
			return nil, err
		}
		found = true
	}
	if found {
		return &ent, nil
	} else {
		return nil, nil
	}
}
