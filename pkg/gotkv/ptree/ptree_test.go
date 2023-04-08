package ptree

import (
	"context"
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
)

func TestBuilder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	b := NewBuilder(BuilderParams[Entry, cadata.ID]{
		Store:           s,
		MeanSize:        defaultAvgSize,
		MaxSize:         defaultMaxSize,
		Seed:            nil,
		Compare:         compareEntries,
		NewEncoder:      NewEntryEncoder,
		NewIndexEncoder: NewIndexEncoder,
		Copy:            copyEntry,
	})

	generateEntries(1e4, func(ent Entry) {
		err := b.Put(ctx, ent)
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
	b := NewBuilder(BuilderParams[Entry, cadata.ID]{
		Store:           s,
		MeanSize:        defaultAvgSize,
		MaxSize:         defaultMaxSize,
		Seed:            nil,
		NewEncoder:      NewEntryEncoder,
		NewIndexEncoder: NewIndexEncoder,
		Compare:         compareEntries,
		Copy:            copyEntry,
	})

	const N = 1e4
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Logf("produced %d blobs", s.Len())

	it := NewIterator(IteratorParams[Entry, cadata.ID]{
		Store:           s,
		Compare:         compareEntries,
		NewDecoder:      NewEntryDecoder,
		NewIndexDecoder: NewIndexDecoder,
		Root:            *root,
		Span:            state.TotalSpan[Entry](),
		Copy:            copyEntry,
	})
	var ent Entry
	for i := 0; i < N; i++ {
		err := it.Next(ctx, &ent)
		require.NoError(t, err, "at %d", i)
		require.Contains(t, string(ent.Key), strconv.Itoa(i))
	}
	for i := 0; i < 3; i++ {
		require.ErrorIs(t, it.Next(ctx, &ent), EOS)
	}
}

func TestCopy(t *testing.T) {
	t.Parallel()
	averageSize := 1 << 12
	maxSize := 1 << 16
	s := cadata.NewMem(cadata.DefaultHash, maxSize)
	ctx := context.Background()
	b := NewBuilder(BuilderParams[Entry, cadata.ID]{
		Store:           s,
		MeanSize:        averageSize,
		MaxSize:         maxSize,
		Seed:            nil,
		Compare:         compareEntries,
		NewEncoder:      NewEntryEncoder,
		NewIndexEncoder: NewIndexEncoder,
		Copy:            copyEntry,
	})
	const N = 1e6
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Log("begin copying")
	it := NewIterator(IteratorParams[Entry, cadata.ID]{
		Store:           s,
		Compare:         compareEntries,
		NewDecoder:      NewEntryDecoder,
		NewIndexDecoder: NewIndexDecoder,
		Root:            *root,
		Span:            state.Span[Entry]{},
		Copy:            copyEntry,
	})
	b2 := NewBuilder(BuilderParams[Entry, cadata.ID]{
		Store:           s,
		MeanSize:        averageSize,
		MaxSize:         maxSize,
		Seed:            nil,
		NewEncoder:      NewEntryEncoder,
		NewIndexEncoder: NewIndexEncoder,
		Compare:         compareEntries,
		Copy:            copyEntry,
	})
	require.NoError(t, Copy(ctx, b2, it))
	root2, err := b2.Finish(ctx)
	require.NoError(t, err)
	require.Equal(t, root, root2)
}

func copyEntry(dst *Entry, src Entry) {
	dst.Key = append(dst.Key[:0], src.Key...)
	dst.Value = append(dst.Value[:0], src.Value...)
}
