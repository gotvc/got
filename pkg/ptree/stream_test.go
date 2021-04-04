package ptree

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/got/pkg/cadata"
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
	var refs []Ref

	s := cadata.NewMem()
	sw := NewStreamWriter(s, func(ctx context.Context, ref Ref, firstKey []byte) error {
		refs = append(refs, ref)
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
			sr = NewStreamReader(s, ref)
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
	s := blobs.Void{}
	sw := NewStreamWriter(s, func(ctx context.Context, ref Ref, lastKey []byte) error {
		return nil
	})
	const N = 1e4
	key := []byte("sample key")
	value := make([]byte, 128)
	for i := 0; i < N; i++ {
		ent := Entry{Key: key, Value: value}
		err := sw.Append(ctx, ent)
		require.NoError(b, err)
	}
	require.NoError(b, sw.Flush(ctx))
}

func TestStreamEditor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := cadata.NewMem()
	// construct initial stream
	var refs1 []Ref
	w := NewStreamWriter(s, func(ctx context.Context, ref Ref, firstKey []byte) error {
		refs1 = append(refs1, ref)
		return nil
	})
	const N = 1e4
	generateEntries(N, func(ent Entry) {
		err := w.Append(ctx, ent)
		require.NoError(t, err)
	})

	// we want to replace a single value at key 7
	key := keyFromInt(7)
	putFn := put(key, []byte("the new inserted value"))

	var refs2 []Ref
	se := NewStreamEditor(s, SingleItemSpan(key), putFn, func(ctx context.Context, ref Ref, _ []byte) error {
		refs2 = append(refs2, ref)
		return nil
	})

	for i := 0; i < len(refs1); i++ {
		ref := refs1[i]
		err := se.Process(ctx, ref)
		require.NoError(t, err)
	}
	require.NoError(t, se.Finish(ctx))

	similar := refSimilarity(refs1, refs2)
	t.Log("refs1:", len(refs1), "refs2:", len(refs2), "common:", similar)
	withinTolerance(t, len(refs1)+len(refs2), 2*similar, 0.25)
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
