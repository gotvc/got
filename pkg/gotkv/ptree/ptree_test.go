package ptree

import (
	"context"
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
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

func TestCopy(t *testing.T) {
	averageSize := 1 << 12
	maxSize := 1 << 16
	t.Parallel()
	ctx := context.Background()
	s := cadata.NewMem(cadata.DefaultHash, maxSize)
	op := gdat.NewOperator()
	b := NewBuilder(s, &op, averageSize, maxSize, nil)
	const N = 1e6
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent.Key, ent.Value)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Log("being copying")
	it := NewIterator(s, &op, *root, Span{})
	b2 := NewBuilder(s, &op, averageSize, maxSize, nil)
	require.NoError(t, Copy(ctx, b2, it))
	root2, err := b2.Finish(ctx)
	require.NoError(t, err)
	require.Equal(t, root, root2)
}
