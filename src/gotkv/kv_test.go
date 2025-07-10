package gotkv

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
)

func TestAddPrefix(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	s := cadata.NewMem(cadata.DefaultHash, 1<<16)
	ag := NewMachine(1<<13, 1<<16)
	b := ag.NewBuilder(s)

	const N = 1e4
	generateEntries(N, func(ent Entry) {
		err := b.Put(ctx, ent.Key, ent.Value)
		require.NoError(t, err)
	})
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)

	prefix := []byte("abc")
	root2 := AddPrefix(*root, prefix)

	t.Logf("produced %d blobs", s.Len())

	it := ag.NewIterator(s, root2, TotalSpan())
	var ent Entry
	for i := 0; i < N; i++ {
		err := it.Next(ctx, &ent)
		require.NoError(t, err, "at %d", i)
		require.True(t, bytes.HasPrefix(ent.Key, prefix), "at %d: %q does not have prefix %q", i, ent.Key, prefix)
	}
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
