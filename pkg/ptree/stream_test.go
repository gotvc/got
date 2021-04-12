package ptree

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"testing"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/stretchr/testify/require"
)

func TestEntry(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	expected := Entry{
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	writeEntry(buf, expected)
	actual, err := readEntry(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, expected, *actual)
}

func TestStreamRW(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	op := gdat.NewOperator()
	var refs []Ref

	s := cadata.NewMem()
	sw := NewStreamWriter(s, op, defaultAvgSize, defaultMaxSize, func(idx Index) error {
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

	var sr *StreamReader
	for i := 0; i < N; i++ {
		if sr == nil {
			ref := refs[0]
			refs = refs[1:]
			sr = NewStreamReader(s, Index{Ref: ref})
		}
		ent, err := sr.Next(ctx)
		if err == io.EOF {
			sr = nil
			i--
			continue
		}
		require.NoError(t, err)
		require.Equal(t, string(keyFromInt(i)), string(ent.Key))
	}
	_, err = sr.Next(ctx)
	require.Equal(t, io.EOF, err)
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
	s := blobs.Void{}
	sw := NewStreamWriter(s, op, defaultAvgSize, defaultMaxSize, func(idx Index) error {
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
	require.True(t, ok)
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
