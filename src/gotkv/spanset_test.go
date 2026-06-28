package gotkv

import (
	"context"
	"testing"

	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/exp/streams"
)

func TestSpanSet(t *testing.T) {
	type TestCase struct {
		Name  string
		Spans []Span
		Want  []Span
	}

	s := func(begin, end string) Span {
		return Span{Begin: []byte(begin), End: []byte(end)}
	}

	testCases := []TestCase{
		{
			Name:  "empty",
			Spans: nil,
			Want:  nil,
		},
		{
			Name: "single-span",
			Spans: []Span{
				s("a", "b"),
			},
			Want: []Span{
				s("a", "b"),
			},
		},
		{
			Name: "non-overlapping",
			Spans: []Span{
				s("a", "c"),
				s("d", "f"),
			},
			Want: []Span{
				s("a", "c"),
				s("d", "f"),
			},
		},
		{
			Name: "touching-should-combine",
			Spans: []Span{
				s("a", "c"),
				s("c", "e"),
			},
			Want: []Span{
				s("a", "e"),
			},
		},
		{
			Name: "overlapping-extends",
			Spans: []Span{
				s("a", "d"),
				s("c", "f"),
			},
			Want: []Span{
				s("a", "f"),
			},
		},
		{
			Name: "contained-noop",
			Spans: []Span{
				s("a", "f"),
				s("c", "d"),
			},
			Want: []Span{
				s("a", "f"),
			},
		},
		{
			Name: "multiple-touching-chained",
			Spans: []Span{
				s("a", "c"),
				s("c", "e"),
				s("e", "g"),
			},
			Want: []Span{
				s("a", "g"),
			},
		},
		{
			Name: "mix-non-touching-and-touching",
			Spans: []Span{
				s("a", "c"),
				s("c", "e"),
				s("f", "h"),
				s("h", "j"),
			},
			Want: []Span{
				s("a", "e"),
				s("f", "j"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := testutil.Context(t)
			s := stores.NewMem()
			ag := newTestMachine(t)
			w := ag.NewSpanSetWriter(s)

			for _, span := range tc.Spans {
				require.NoError(t, w.Add(ctx, span))
			}
			ss, err := w.Finish(ctx)
			require.NoError(t, err)

			got := collectSpanSetSpans(t, ctx, ag, s, ss)
			require.Equal(t, len(tc.Want), len(got), "span count mismatch")
			for i := range tc.Want {
				require.Equal(t, string(tc.Want[i].Begin), string(got[i].Begin), "span %d begin", i)
				require.Equal(t, string(tc.Want[i].End), string(got[i].End), "span %d end", i)
			}
		})
	}
}

func collectSpanSetSpans(t testing.TB, ctx context.Context, ag Machine, s stores.RO, ss SpanSet) []Span {
	t.Helper()
	entries, err := streams.Collect[Entry](ctx, ag.NewIterator(s, Root(ss), TotalSpan()), 100)
	require.NoError(t, err)
	require.Equal(t, 0, len(entries)%2, "odd number of span set entries")
	spans := make([]Span, 0, len(entries)/2)
	for i := 0; i < len(entries); i += 2 {
		spans = append(spans, Span{Begin: entries[i].Key, End: entries[i+1].Key})
	}
	return spans
}