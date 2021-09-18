package ptree

import (
	"bytes"
	"context"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
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
	root, err = AddPrefix(ctx, s, *root, prefix)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Logf("produced %d blobs", s.Len())

	it := NewIterator(s, &op, *root, Span{})
	for i := 0; i < N; i++ {
		ent, err := it.Next(ctx)
		require.NoError(t, err, "at %d", i)
		require.True(t, bytes.HasPrefix(ent.Key, prefix))
	}
}
