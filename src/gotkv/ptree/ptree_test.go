package ptree

import (
	"strconv"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/cadata"
)

func TestBuilder(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	b := newBuilder(t, s)

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
	ctx := testutil.Context(t)
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	b := newBuilder(t, s)

	const N = 1e4
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Logf("produced %d blobs", s.Len())

	it := newIterator(t, s, *root, state.TotalSpan[Entry]())

	var ent Entry
	for i := 0; i < N; i++ {
		err := streams.NextUnit(ctx, it, &ent)
		require.NoError(t, err, "at %d", i)
		require.Contains(t, string(ent.Key), strconv.Itoa(i))
	}
	for i := 0; i < 3; i++ {
		require.ErrorIs(t, streams.NextUnit(ctx, it, &ent), streams.EOS())
	}
}

func TestIterateEmptySpan(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	b := newBuilder(t, s)

	const N = 1e4
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	span := state.TotalSpan[Entry]().
		WithLowerIncl(Entry{Key: keyFromInt(int(N) * 3 / 4)}).
		WithUpperExcl(Entry{Key: keyFromInt(int(N) * 1 / 4)})
	it := newIterator(t, s, *root, span)

	var ent Entry
	for i := 0; i < 3; i++ {
		require.ErrorIs(t, streams.NextUnit(ctx, it, &ent), streams.EOS())
	}
}

func TestCopy(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	maxSize := 1 << 16
	s := cadata.NewMem(cadata.DefaultHash, maxSize)
	b := newBuilder(t, s)

	const N = 1e6
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Log("begin copying")
	t.Log("root", root.String())
	it := newIterator(t, s, *root, state.Span[Entry]{})
	b2 := newBuilder(t, s)

	require.NoError(t, Copy(ctx, b2, it))
	root2, err := b2.Finish(ctx)
	require.NoError(t, err)
	t.Log("root2", root2.String())

	it2 := newIterator(t, s, *root2, state.Span[Entry]{})
	var ent Entry
	for i := 0; i < N; i++ {
		require.NoError(t, streams.NextUnit(ctx, it2, &ent))
		require.Equal(t, keyFromInt(i), ent.Key)
	}
	require.ErrorIs(t, streams.NextUnit(ctx, it2, &ent), streams.EOS())
	require.Equal(t, root, root2)
}

func TestCopySpan(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	maxSize := 1 << 16
	s := cadata.NewMem(cadata.DefaultHash, maxSize)
	b := newBuilder(t, s)

	const N = 1e6
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)

	// copy a span to
	begin := int(N) * 1 / 3
	end := int(N) * 2 / 3
	span := state.TotalSpan[Entry]().
		WithLowerIncl(Entry{Key: keyFromInt(begin)}).
		WithUpperExcl(Entry{Key: keyFromInt(end)})
	it := newIterator(t, s, *root, span)
	b2 := newBuilder(t, s)
	t.Log("copying from", begin, "to", end)
	require.NoError(t, Copy(ctx, b2, it))
	root2, err := b2.Finish(ctx)
	require.NoError(t, err)

	it2 := newIterator(t, s, *root2, state.TotalSpan[Entry]())

	var ent Entry
	for i := begin; i < end; i++ {
		require.NoError(t, streams.NextUnit(ctx, it2, &ent))
		require.Equal(t, keyFromInt(i), ent.Key)
	}
	require.ErrorIs(t, streams.NextUnit(ctx, it2, &ent), streams.EOS())
}

func TestCopyMultiple(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	maxSize := 1 << 16
	s := cadata.NewMem(cadata.DefaultHash, maxSize)
	b := newBuilder(t, s)

	const N = 1e6
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)

	it1 := newIterator(t, s, *root, state.TotalSpan[Entry]().WithUpperExcl(Entry{Key: keyFromInt(int(N) * 1 / 3)}))
	it2 := newIterator(t, s, *root, state.TotalSpan[Entry]().WithLowerIncl(Entry{Key: keyFromInt(int(N) * 2 / 3)}))

	b2 := newBuilder(t, s)
	require.NoError(t, Copy(ctx, b2, it1))
	require.NoError(t, Copy(ctx, b2, it2))
	root2, err := b2.Finish(ctx)
	require.NoError(t, err)

	itFinal := newIterator(t, s, *root2, state.TotalSpan[Entry]())
	var ent Entry
	for i := 0; i < int(N)*1/3; i++ {
		require.NoError(t, streams.NextUnit(ctx, itFinal, &ent))
		require.Equal(t, keyFromInt(i), ent.Key)
	}
	for i := int(N) * 2 / 3; i < N; i++ {
		require.NoError(t, streams.NextUnit(ctx, itFinal, &ent))
		require.Equal(t, keyFromInt(i), ent.Key)
	}
	require.ErrorIs(t, streams.NextUnit(ctx, itFinal, &ent), streams.EOS())
}

// TestSeek checks that the iterator can Seek to entries which exist in the tree.
func TestSeek(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	maxSize := 1 << 16
	s := cadata.NewMem(cadata.DefaultHash, maxSize)
	b := newBuilder(t, s)

	const N = 1e6
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	it := newIterator(t, s, *root, state.Span[Entry]{})
	for _, n := range []int{250, 500, 750, 752, 2000, 10_000} {
		require.NoError(t, it.Seek(ctx, Entry{Key: keyFromInt(n)}))

		var ent Entry
		require.NoError(t, streams.NextUnit(ctx, it, &ent))
		require.Equal(t, string(keyFromInt(n)), string(ent.Key))
		require.Equal(t, string(valueFromInt(n)), string(ent.Value))
	}
}

// TestSeekNotExists checks that seeking to an entry that does not exist
// advances the iterator to the entry immediately after the non-existant entry.
func TestSeekNonExist(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	maxSize := 1 << 16
	s := cadata.NewMem(cadata.DefaultHash, maxSize)
	b := newBuilder(t, s)

	const N = 1e6
	generateEntries(N, func(ent Entry) {
		ent.Key = append(ent.Key, []byte("---")...)
		err := b.Put(ctx, ent)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	it := newIterator(t, s, *root, state.Span[Entry]{})
	for _, n := range []int{250, 500, 750, 752, 2000, 10_000} {
		require.NoError(t, it.Seek(ctx, Entry{Key: keyFromInt(n)}))

		var ent Entry
		require.NoError(t, streams.NextUnit(ctx, it, &ent))
		k := append(keyFromInt(n), []byte("---")...)
		require.Equal(t, string(k), string(ent.Key))
		require.Equal(t, string(valueFromInt(n)), string(ent.Value))
	}
}

func TestEmpty(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	maxSize := 1 << 16
	s := cadata.NewMem(cadata.DefaultHash, maxSize)
	b := newBuilder(t, s)

	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.Equal(t, uint8(0), root.Depth)

	it := newIterator(t, s, *root, state.TotalSpan[Entry]())
	for i := 0; i < 10; i++ {
		require.ErrorIs(t, streams.NextUnit(ctx, it, &Entry{}), streams.EOS())
	}
}

func newBuilder(t testing.TB, s cadata.Store) *Builder[Entry, blobcache.CID] {
	averageSize := 1 << 12
	return NewBuilder(BuilderParams[Entry, blobcache.CID]{
		Store:           s,
		MeanSize:        averageSize,
		MaxSize:         s.MaxSize(),
		Seed:            nil,
		Compare:         compareEntries,
		NewEncoder:      NewEntryEncoder,
		NewIndexEncoder: NewIndexEncoder,
		Copy:            copyEntry,
	})
}

func newIterator(t testing.TB, s cadata.Store, root Root[Entry, blobcache.CID], span state.Span[Entry]) *Iterator[Entry, blobcache.CID] {
	return NewIterator(IteratorParams[Entry, blobcache.CID]{
		Store:           s,
		Compare:         compareEntries,
		Copy:            copyEntry,
		NewDecoder:      NewEntryDecoder,
		NewIndexDecoder: NewIndexDecoder,
		Root:            root,
		Span:            span,
	})
}
