package ptree

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kv"
	"github.com/stretchr/testify/require"
)

const (
	defaultMaxSize = 1 << 21
	defaultAvgSize = 1 << 13
)

func TestEntry(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	expected := Entry{
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	writeEntry(buf, nil, expected)
	var actual Entry
	err := readEntry(&actual, bytes.NewReader(buf.Bytes()), nil, defaultMaxSize)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestStreamRW(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	op := gdat.NewOperator()
	var refs []Ref
	var idxs []Index

	s := cadata.NewMem(cadata.DefaultHash, defaultMaxSize)
	sw := NewStreamWriter(s, &op, defaultAvgSize, defaultMaxSize, nil, func(idx Index) error {
		idxs = append(idxs, idx)
		refs = append(refs, idx.Ref)
		return nil
	})

	const N = 1e4
	generateEntries(N, func(ent Entry) {
		err := sw.Append(ctx, ent)
		require.NoError(t, err)
	})
	err := sw.Flush(ctx)
	require.NoError(t, err)

	sr := NewStreamReader(s, &op, idxs)
	var ent Entry
	for i := 0; i < N; i++ {
		err := sr.Next(ctx, &ent)
		require.NoError(t, err)
		require.Equal(t, string(keyFromInt(i)), string(ent.Key))
	}
	err = sr.Next(ctx, &ent)
	require.Equal(t, kv.EOS, err)
}

func TestStreamWriterChunkSize(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	op := gdat.NewOperator()
	var refs []Ref

	s := cadata.NewMem(cadata.DefaultHash, defaultMaxSize)
	sw := NewStreamWriter(s, &op, defaultAvgSize, defaultMaxSize, nil, func(idx Index) error {
		refs = append(refs, idx.Ref)
		return nil
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
	for _, ref := range refs {
		err := cadata.GetF(ctx, s, ref.CID, func(data []byte) error {
			total += len(data)
			return nil
		})
		require.NoError(t, err)
	}
	avgSize := total / count
	withinTolerance(t, avgSize, defaultAvgSize, 0.1)
}

func generateEntries(n int, fn func(ent Entry)) {
	for i := 0; i < n; i++ {
		fn(Entry{
			Key:   keyFromInt(i),
			Value: []byte("test value" + strconv.Itoa(i)),
		})
	}
}

func keyFromInt(i int) []byte {
	return []byte(fmt.Sprintf("%010d", i))
}

func BenchmarkStreamWriter(b *testing.B) {
	b.ReportAllocs()

	ctx := context.Background()
	op := gdat.NewOperator()
	s := cadata.Void{}
	sw := NewStreamWriter(s, &op, defaultAvgSize, defaultMaxSize, nil, func(idx Index) error {
		return nil
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

func refSimilarity(as, bs []Ref) int {
	am := map[cadata.ID]struct{}{}
	for _, ref := range as {
		am[ref.CID] = struct{}{}
	}
	var count int
	for _, ref := range bs {
		if _, exists := am[ref.CID]; exists {
			count++
		}
	}
	return count
}
