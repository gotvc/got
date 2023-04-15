package ptree

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/streams"
	"github.com/stretchr/testify/require"
)

const (
	defaultMaxSize = 1 << 21
	defaultAvgSize = 1 << 13
)

func TestEntry(t *testing.T) {
	t.Parallel()
	buf := make([]byte, 1<<10)
	expected := Entry{
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	enc := NewEntryEncoder()
	n, err := enc.Write(buf, expected)
	require.NoError(t, err)

	var actual Entry
	dec := NewEntryDecoder()
	n2, err := dec.Read(buf[:n], &actual)
	require.NoError(t, err)

	require.Equal(t, n, n2)
	require.Equal(t, expected, actual)
}

func TestStreamRW(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var refs []cadata.ID
	var idxs []Index[Entry, cadata.ID]

	s := cadata.NewMem(cadata.DefaultHash, defaultMaxSize)
	sw := NewStreamWriter(StreamWriterParams[Entry, cadata.ID]{
		Store:    s,
		Compare:  compareEntries,
		MeanSize: defaultAvgSize,
		MaxSize:  defaultMaxSize,
		Seed:     nil,
		Encoder:  NewEntryEncoder(),
		Copy:     copyEntry,
		OnIndex: func(idx Index[Entry, cadata.ID]) error {
			idxs = append(idxs, cloneIndex(idx))
			refs = append(refs, idx.Ref)
			return nil
		},
	})

	const N = 1e4
	generateEntries(N, func(ent Entry) {
		err := sw.Append(ctx, ent)
		require.NoError(t, err)
	})
	err := sw.Flush(ctx)
	require.NoError(t, err)

	sr := NewStreamReader(StreamReaderParams[Entry, cadata.ID]{
		Store:     s,
		Compare:   compareEntries,
		NextIndex: NextIndexFromSlice(idxs),
		Decoder:   NewEntryDecoder(),
	})
	var ent Entry
	for i := 0; i < N; i++ {
		err := sr.Next(ctx, &ent)
		require.NoError(t, err)
		require.Equal(t, string(keyFromInt(i)), string(ent.Key))
	}
	err = sr.Next(ctx, &ent)
	require.Equal(t, streams.EOS(), err)
}

func TestStreamWriterChunkSize(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var refs []cadata.ID

	s := cadata.NewMem(cadata.DefaultHash, defaultMaxSize)
	sw := NewStreamWriter(StreamWriterParams[Entry, cadata.ID]{
		Store:    s,
		MeanSize: defaultAvgSize,
		MaxSize:  defaultMaxSize,
		Seed:     nil,
		Compare:  compareEntries,
		Encoder:  NewEntryEncoder(),
		OnIndex: func(idx Index[Entry, cadata.ID]) error {
			refs = append(refs, idx.Ref)
			return nil
		},
		Copy: copyEntry,
	})

	const N = 1e5
	generateEntries(N, func(ent Entry) {
		err := sw.Append(ctx, ent)
		require.NoError(t, err)
	})
	err := sw.Flush(ctx)
	require.NoError(t, err)

	count := len(refs)
	t.Log("count:", count)
	var total int
	buf := make([]byte, sw.p.MaxSize)
	for _, ref := range refs {
		n, err := s.Get(ctx, ref, buf)
		require.NoError(t, err)
		total += n
	}
	avgSize := total / count
	withinTolerance(t, avgSize, defaultAvgSize, 0.1)
}

func TestStreamSeek(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var refs []cadata.ID
	var idxs []Index[Entry, cadata.ID]

	s := cadata.NewMem(cadata.DefaultHash, defaultMaxSize)
	sw := NewStreamWriter(StreamWriterParams[Entry, cadata.ID]{
		Store:    s,
		Compare:  compareEntries,
		MeanSize: defaultAvgSize,
		MaxSize:  defaultMaxSize,
		Seed:     nil,
		Encoder:  NewEntryEncoder(),
		Copy:     copyEntry,
		OnIndex: func(idx Index[Entry, cadata.ID]) error {
			idxs = append(idxs, cloneIndex(idx))
			refs = append(refs, idx.Ref)
			return nil
		},
	})

	const N = 1e4
	generateEntries(N, func(ent Entry) {
		err := sw.Append(ctx, ent)
		require.NoError(t, err)
	})
	err := sw.Flush(ctx)
	require.NoError(t, err)

	sr := NewStreamReader(StreamReaderParams[Entry, cadata.ID]{
		Store:     s,
		Compare:   compareEntries,
		NextIndex: NextIndexFromSlice(idxs),
		Decoder:   NewEntryDecoder(),
	})

	for _, n := range []int{50, 100, 250, 500, 750, 751, 753, 5000} {
		require.NoError(t, sr.Seek(ctx, Entry{Key: keyFromInt(n)}))

		var ent Entry
		require.NoError(t, sr.Next(ctx, &ent))
		require.Equal(t, string(keyFromInt(n)), string(ent.Key))
		require.Equal(t, string(valueFromInt(n)), string(ent.Value))
	}
}

func generateEntries(n int, fn func(ent Entry)) {
	for i := 0; i < n; i++ {
		fn(Entry{
			Key:   keyFromInt(i),
			Value: valueFromInt(i),
		})
	}
}

func keyFromInt(i int) []byte {
	return []byte(fmt.Sprintf("%010d", i))
}

func valueFromInt(i int) []byte {
	return []byte("test value" + strconv.Itoa(i))
}

func BenchmarkStreamWriter(b *testing.B) {
	b.ReportAllocs()

	ctx := context.Background()
	s := cadata.NewVoid(cadata.DefaultHash, defaultMaxSize)
	sw := NewStreamWriter(StreamWriterParams[Entry, cadata.ID]{
		Store:    s,
		MeanSize: defaultAvgSize,
		MaxSize:  defaultMaxSize,
		Seed:     nil,
		Encoder:  NewEntryEncoder(),
		OnIndex:  func(idx Index[Entry, cadata.ID]) error { return nil },
		Copy:     copyEntry,
		Compare:  compareEntries,
	})
	generateEntries(b.N, func(ent Entry) {
		err := sw.Append(ctx, ent)
		require.NoError(b, err)
	})
	require.NoError(b, sw.Flush(ctx))
}

func withinTolerance(t *testing.T, x int, target int, tol float64) {
	ok := math.Abs(float64(x)-float64(target)) < float64(target)*tol
	if !ok {
		t.Errorf("value (%d) not within tolerance (+/- %f) of target (%d)", x, tol, target)
	}
}

func refSimilarity[Ref comparable](as, bs []Ref) int {
	am := map[Ref]struct{}{}
	for _, ref := range as {
		am[ref] = struct{}{}
	}
	var count int
	for _, ref := range bs {
		if _, exists := am[ref]; exists {
			count++
		}
	}
	return count
}

func cloneIndex(x Index[Entry, cadata.ID]) Index[Entry, cadata.ID] {
	return Index[Entry, cadata.ID]{
		Ref:       x.Ref,
		IsNatural: x.IsNatural,
		Span:      cloneSpan(x.Span, copyEntry),
	}
}
