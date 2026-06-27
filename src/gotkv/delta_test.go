package gotkv

import (
	"context"
	"testing"

	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/exp/streams"
)

type dwSegment struct {
	Span    Span
	Entries []Entry
}

func TestDeltaWriter(t *testing.T) {
	type TestCase struct {
		Name    string
		Edits   []Edit
		WantErr bool
		Want    []dwSegment
	}

	makeEdit := func(begin, end string, entries ...Entry) Edit {
		return Edit{
			Span:    Span{Begin: []byte(begin), End: []byte(end)},
			Entries: entries,
		}
	}
	mkSpan := func(begin, end string) Span {
		return Span{Begin: []byte(begin), End: []byte(end)}
	}
	mkEnt := func(key, value string) Entry {
		return Entry{Key: []byte(key), Value: []byte(value)}
	}

	testCases := []TestCase{
		{
			Name: "non-overlapping",
			Edits: []Edit{
				makeEdit("a", "c", mkEnt("a", "1")),
				makeEdit("d", "f", mkEnt("d", "2")),
			},
			Want: []dwSegment{
				{Span: mkSpan("a", "c"), Entries: []Entry{mkEnt("a", "1")}},
				{Span: mkSpan("d", "f"), Entries: []Entry{mkEnt("d", "2")}},
			},
		},
		{
			Name: "touching-should-combine",
			Edits: []Edit{
				makeEdit("a", "c", mkEnt("a", "1")),
				makeEdit("c", "e", mkEnt("c", "2")),
			},
			Want: []dwSegment{
				{Span: mkSpan("a", "e"), Entries: []Entry{mkEnt("a", "1"), mkEnt("c", "2")}},
			},
		},
		{
			Name: "overlapping",
			Edits: []Edit{
				makeEdit("a", "d", mkEnt("a", "1")),
				makeEdit("c", "f", mkEnt("c", "2")),
			},
			WantErr: true,
		},
		{
			Name: "multiple-touching-chained",
			Edits: []Edit{
				makeEdit("a", "c", mkEnt("a", "1")),
				makeEdit("c", "e", mkEnt("c", "2")),
				makeEdit("e", "g", mkEnt("e", "3")),
			},
			Want: []dwSegment{
				{Span: mkSpan("a", "g"), Entries: []Entry{mkEnt("a", "1"), mkEnt("c", "2"), mkEnt("e", "3")}},
			},
		},
		{
			Name: "contained-overlap",
			Edits: []Edit{
				makeEdit("a", "f", mkEnt("a", "1")),
				makeEdit("c", "d", mkEnt("c", "2")),
			},
			WantErr: true,
		},
		{
			Name: "single-edit",
			Edits: []Edit{
				makeEdit("a", "b", mkEnt("a", "1")),
			},
			Want: []dwSegment{
				{Span: mkSpan("a", "b"), Entries: []Entry{mkEnt("a", "1")}},
			},
		},
		{
			Name:  "empty",
			Edits: nil,
			Want:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := testutil.Context(t)
			s := stores.NewMem()
			ag := newTestMachine(t)
			dw := ag.NewDeltaWriter(s)

			var gotErr error
			for _, edit := range tc.Edits {
				if err := dw.AddEdit(ctx, edit); err != nil {
					gotErr = err
					break
				}
			}
			if tc.WantErr {
				require.Error(t, gotErr)
				return
			}
			require.NoError(t, gotErr)
			delta, err := dw.Finish(ctx)
			require.NoError(t, err)
			require.NotZero(t, delta)

			got := collectDeltaSegments(t, ctx, ag, s, delta)
			require.Equal(t, len(tc.Want), len(got), "segment count mismatch")
			for i := range tc.Want {
				require.Equal(t, string(tc.Want[i].Span.Begin), string(got[i].Span.Begin), "segment %d span begin", i)
				require.Equal(t, string(tc.Want[i].Span.End), string(got[i].Span.End), "segment %d span end", i)
				require.Equal(t, len(tc.Want[i].Entries), len(got[i].Entries), "segment %d entry count", i)
				for j := range tc.Want[i].Entries {
					require.Equal(t, string(tc.Want[i].Entries[j].Key), string(got[i].Entries[j].Key), "segment %d entry %d key", i, j)
					require.Equal(t, string(tc.Want[i].Entries[j].Value), string(got[i].Entries[j].Value), "segment %d entry %d value", i, j)
				}
			}
		})
	}
}

func collectDeltaSegments(t testing.TB, ctx context.Context, ag Machine, s stores.RO, d Delta) []dwSegment {
	t.Helper()
	segments, err := streams.Collect[Segment](ctx, ag.NewDeltaReader(s, d), 100)
	require.NoError(t, err)
	out := make([]dwSegment, len(segments))
	for i, seg := range segments {
		entries, err := streams.Collect[Entry](ctx, ag.NewIterator(s, seg.Contents, TotalSpan()), 100)
		require.NoError(t, err)
		out[i] = dwSegment{Span: seg.Span, Entries: entries}
	}
	return out
}
