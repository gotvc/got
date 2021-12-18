package ptree

import (
	"bytes"
	"context"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/stretchr/testify/require"
)

func TestAddPrefix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := cadata.NewMem(cadata.DefaultHash, defaultMaxSize)
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

	prefix := []byte("abc")
	root2 := AddPrefix(*root, prefix)

	t.Logf("produced %d blobs", s.Len())

	it := NewIterator(s, &op, root2, kvstreams.TotalSpan())
	var ent Entry
	for i := 0; i < N; i++ {
		err := it.Next(ctx, &ent)
		require.NoError(t, err, "at %d", i)
		require.True(t, bytes.HasPrefix(ent.Key, prefix), "at %d: %q does not have prefix %q", i, ent.Key, prefix)
	}
}
