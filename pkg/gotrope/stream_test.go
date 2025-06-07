package gotrope

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
)

var ctx = context.Background()

func TestWriteRead(t *testing.T) {
	const N = 10000
	s := newStore(t)
	var refs []Ref
	sw := NewStreamWriter(s, defaultMeanSize, defaultMaxSize, new([16]byte), func(ctx context.Context, idx Index[Ref]) error {
		refs = append(refs, idx.Ref)
		return nil
	})
	var v []byte
	for i := 0; i < N; i++ {
		v = fmt.Appendf(v[:0], "hello world %d", i)
		err := sw.Append(ctx, StreamEntry{Weight: Weight{1}, Value: v})
		require.NoError(t, err)
	}
	require.NoError(t, sw.Flush(ctx))
	require.Greater(t, s.(writeStore).s.(*cadata.MemStore).Len(), 2)

	sr := NewStreamReader[Ref](s, func(context.Context) (*cadata.ID, error) {
		if len(refs) == 0 {
			return nil, nil
		}
		r := refs[0]
		refs = refs[1:]
		return &r, nil
	})

	var ent StreamEntry
	for i := 0; i < N; i++ {
		expectV := []byte("hello world " + strconv.Itoa(i))
		require.NoError(t, sr.Next(ctx, &ent))
		require.Equal(t, Weight{1}, ent.Weight)
		require.Equal(t, expectV, ent.Value)
	}
	require.ErrorIs(t, sr.Next(ctx, &ent), EOS())
}

func TestEntryWrite(t *testing.T) {
	var out []byte

	w := Weight{3}
	data := []byte("hello world")
	l := entryEncodedLen(w, data)
	out = appendEntry(out, StreamEntry{Weight: w, Value: data})
	require.Len(t, out, l)

	var ent StreamEntry
	l2, err := parseEntry(&ent, out)
	require.NoError(t, err)
	require.Equal(t, l, l2)
	require.Equal(t, w, ent.Weight)
	require.Equal(t, data, ent.Value)
}
