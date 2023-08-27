package gotkv

import (
	"testing"

	"github.com/brendoncarroll/go-exp/maybe"
	"github.com/brendoncarroll/go-exp/streams"
	"github.com/gotvc/got/pkg/stores"
	"github.com/gotvc/got/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDiff(t *testing.T) {
	ctx := testutil.Context(t)
	ag := newTestAgent(t)
	s := stores.NewMem()

	lb := ag.NewBuilder(s)
	require.NoError(t, lb.Put(ctx, []byte("a"), []byte("1")))
	require.NoError(t, lb.Put(ctx, []byte("b"), []byte("2")))
	require.NoError(t, lb.Put(ctx, []byte("c"), []byte("3")))
	left, err := lb.Finish(ctx)
	require.NoError(t, err)

	rb := ag.NewBuilder(s)
	require.NoError(t, rb.Put(ctx, []byte("a"), []byte("1")))
	require.NoError(t, rb.Put(ctx, []byte("b"), []byte("2222")))
	require.NoError(t, rb.Put(ctx, []byte("c"), []byte("3")))
	require.NoError(t, rb.Put(ctx, []byte("d"), []byte("4")))
	right, err := rb.Finish(ctx)
	require.NoError(t, err)

	d := ag.NewDiffer(s, *left, *right, TotalSpan())
	dents, err := streams.Collect[DEntry](ctx, d, 6)
	require.NoError(t, err)
	require.Equal(t, []DEntry{
		{
			Key:   []byte("b"),
			Left:  maybe.Just([]byte("2")),
			Right: maybe.Just([]byte("2222")),
		},
		{
			Key:   []byte("d"),
			Left:  maybe.Nothing[[]byte](),
			Right: maybe.Just([]byte("4")),
		},
	}, dents)

	// only return EOS for subsequent calls
	for i := 0; i < 10; i++ {
		require.True(t, streams.IsEOS(d.Next(ctx, nil)))
	}
}
