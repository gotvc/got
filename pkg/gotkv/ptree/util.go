package ptree

import (
	"context"

	"github.com/brendoncarroll/go-exp/maybe"
	"github.com/brendoncarroll/go-exp/streams"
)

// MaxEntry returns the entry in span with the greatest (ordered last) key.
func MaxEntry[T, Ref any](ctx context.Context, params ReadParams[T, Ref], x Root[T, Ref], lt maybe.Maybe[T], dst *T) error {
	if x.Depth == 0 {
		sr := NewStreamReader(StreamReaderParams[T, Ref]{
			Store:     params.Store,
			Compare:   params.Compare,
			Decoder:   params.NewDecoder(),
			NextIndex: NextIndexFromSlice([]Index[T, Ref]{x.Index}),
		})
		return maxEntry(ctx, sr, params.Compare, lt, dst)
	} else {
		idxs, err := ListIndexes(ctx, params, x)
		if err != nil {
			return err
		}
		for i := len(idxs) - 1; i >= 0; i-- {
			idx := idxs[i]
			if lt.Ok && idx.Span.Compare(lt.X, params.Compare) > 0 {
				// the span is strictly above the less than value.
				// we need to keep descending to find a suitable value
				//
				// not performing this check would be inefficient, but also correct.
				continue
			}
			if err := MaxEntry(ctx, params, indexToRoot(idx, x.Depth-1), lt, dst); !streams.IsEOS(err) {
				return err
			}
		}
		return streams.EOS()
	}
}

// maxEntry returns the maximum entry from a StreamReader.
// It performs O(n) calls to Next and Peek.
func maxEntry[T, Ref any](ctx context.Context, sr *StreamReader[T, Ref], cmp func(a, b T) int, under maybe.Maybe[T], dst *T) error {
	var ent T
	var found bool
	for {
		if err := sr.Peek(ctx, &ent); err != nil {
			if streams.IsEOS(err) {
				break
			}
			return err
		}
		if under.Ok && cmp(ent, under.X) >= 0 {
			break // not under
		}
		if err := sr.Next(ctx, dst); err != nil {
			return err
		}
		found = true
	}
	if found {
		return nil
	} else {
		return streams.EOS()
	}
}
