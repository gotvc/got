package ptree

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kv"
	"github.com/stretchr/testify/require"
)

func TestBuilder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	op := gdat.NewOperator()
	b := NewBuilder(s, &op, defaultAvgSize, defaultMaxSize, nil)

	generateEntries(1e4, func(ent Entry) {
		err := b.Put(ctx, ent.Key, ent.Value)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)
}

func TestBuildIterate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	op := gdat.NewOperator()
	b := NewBuilder(s, &op, defaultAvgSize, defaultMaxSize, nil)

	const N = 1e4
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent.Key, ent.Value)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Logf("produced %d blobs", s.Len())

	it := NewIterator(s, &op, *root, Span{})
	var ent Entry
	for i := 0; i < N; i++ {
		err := it.Next(ctx, &ent)
		require.NoError(t, err, "at %d", i)
		require.Contains(t, string(ent.Key), strconv.Itoa(i))
	}
}

func TestMutate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	op := gdat.NewOperator()
	b := NewBuilder(s, &op, defaultAvgSize, defaultMaxSize, nil)

	const N = 1e4
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent.Key, ent.Value)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	k := keyFromInt(int(N) / 3)
	v := []byte("new changed value")
	root, err = Mutate(ctx, NewBuilder(s, &op, defaultAvgSize, defaultMaxSize, nil), *root, Mutation{
		Span: kv.SingleItemSpan(k),
		Fn:   func(*Entry) []Entry { return []Entry{{Key: k, Value: v}} },
	})
	require.NoError(t, err)
	require.NotNil(t, root)

	// check that our key is there
	it := NewIterator(s, &op, *root, kv.SingleItemSpan(k))
	var ent Entry
	err = it.Next(ctx, &ent)
	require.NoError(t, err)
	t.Logf("%q %q", ent.Key, ent.Value)
	require.Equal(t, ent.Key, k)
	require.Equal(t, string(v), string(ent.Value))
	err = it.Next(ctx, &ent)
	require.Equal(t, err, kv.EOS)

	// check that all the other keys are there too
	it = NewIterator(s, &op, *root, kv.TotalSpan())
	generateEntries(N, func(expected Entry) {
		err := it.Next(ctx, &ent)
		require.NoError(t, err)
		if !bytes.Equal(k, ent.Key) {
			require.Equal(t, expected, ent)
		}
	})
	err = it.Next(ctx, &ent)
	require.Equal(t, err, kv.EOS)
}
