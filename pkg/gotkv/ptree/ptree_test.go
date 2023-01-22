package ptree

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
)

func TestBuilder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := wrapStore(cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize))
	b := NewBuilder(BuilderParams{
		Store:      s,
		MeanSize:   defaultAvgSize,
		MaxSize:    defaultMaxSize,
		Seed:       nil,
		Compare:    bytes.Compare,
		NewEncoder: NewJSONEncoder,
	})

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
	s2 := wrapStore(s)
	b := NewBuilder(BuilderParams{
		Store:      s2,
		MeanSize:   defaultAvgSize,
		MaxSize:    defaultMaxSize,
		Seed:       nil,
		Compare:    bytes.Compare,
		NewEncoder: NewJSONEncoder,
	})

	const N = 1e4
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent.Key, ent.Value)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Logf("produced %d blobs", s.Len())

	it := NewIterator(IteratorParams{
		Store:      s2,
		Compare:    bytes.Compare,
		NewDecoder: NewJSONDecoder,
		Root:       *root,
		Span:       Span{},
	})
	var ent Entry
	for i := 0; i < N; i++ {
		err := it.Next(ctx, &ent)
		require.NoError(t, err, "at %d", i)
		require.Contains(t, string(ent.Key), strconv.Itoa(i))
	}
}

func TestCopy(t *testing.T) {
	t.Parallel()
	averageSize := 1 << 12
	maxSize := 1 << 16
	ctx := context.Background()
	s := wrapStore(cadata.NewMem(cadata.DefaultHash, maxSize))
	b := NewBuilder(BuilderParams{
		Store:      s,
		MeanSize:   averageSize,
		MaxSize:    maxSize,
		Seed:       nil,
		Compare:    bytes.Compare,
		NewEncoder: NewJSONEncoder,
	})
	const N = 1e6
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent.Key, ent.Value)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Log("begin copying")
	it := NewIterator(IteratorParams{
		Store:      s,
		Compare:    bytes.Compare,
		NewDecoder: NewJSONDecoder,
		Root:       *root,
		Span:       Span{},
	})
	b2 := NewBuilder(BuilderParams{
		Store:      s,
		MeanSize:   averageSize,
		MaxSize:    maxSize,
		Seed:       nil,
		NewEncoder: NewJSONEncoder,
		Compare:    bytes.Compare,
	})
	require.NoError(t, Copy(ctx, b2, it))
	root2, err := b2.Finish(ctx)
	require.NoError(t, err)
	require.Equal(t, root, root2)
}
