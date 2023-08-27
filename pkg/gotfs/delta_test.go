package gotfs

import (
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-exp/streams"
	"github.com/gotvc/got/pkg/stores"
	"github.com/gotvc/got/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDeltaRW(t *testing.T) {
	tcs := [][]DeltaEntry{
		{
			{Path: "a", PutInfo: &Info{}},
		},
		{
			{Path: "a", Delete: &struct{}{}},
		},
		{
			{Path: "a", PutInfo: &Info{Mode: 0o644}},
			{Path: "b", Delete: &struct{}{}},
			{Path: "c", PutContent: &PutContent{End: 1000}},
		},
	}
	ctx := testutil.Context(t)
	s := stores.NewMem()
	ag := NewAgent()
	for i, tc := range tcs {
		expected := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			b := ag.NewDeltaBuilder(s, s)
			for _, de := range expected {
				err := b.write(ctx, de)
				require.NoError(t, err)
			}
			delta, err := b.Finish(ctx)
			require.NoError(t, err)

			it := ag.NewDeltaIterator(s, s, *delta)
			actual, err := streams.Collect[DeltaEntry](ctx, it, 10)
			require.NoError(t, err)
			require.Equal(t, len(expected), len(actual))
			for i := range expected {
				e := expected[i]
				a := actual[i]
				requireEqualDeltas(t, e, a)
			}
		})
	}
}

func requireEqualDeltas(t testing.TB, e, a DeltaEntry) {
	require.Equal(t, cleanPath(e.Path), cleanPath(a.Path))
	require.Equal(t, e.Delete, a.Delete)
	require.Equal(t, e.PutInfo.marshal(), a.PutInfo.marshal())
	require.Equal(t, e.PutContent, a.PutContent)
}
