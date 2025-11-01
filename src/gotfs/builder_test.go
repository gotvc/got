package gotfs

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"blobcache.io/blobcache/src/schema"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestBuilderMkdir(t *testing.T) {
	ctx, ag, s := setup(t)
	b := ag.NewBuilder(ctx, s, s)
	require.Error(t, b.Mkdir("1", 0o755))
	require.NoError(t, b.Mkdir("", 0o755))
	var p string
	for i := 1; i < 10; i++ {
		p += "/" + strconv.Itoa(i)
		require.NoError(t, b.Mkdir(p, 0o755))
	}
}

func TestBuilderSmallFiles(t *testing.T) {
	ctx, ag, s := setup(t)
	b := ag.NewBuilder(ctx, s, s)
	require.NoError(t, b.Mkdir("", 0o755))
	const N = 1e5
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("%012d", i)
		err := b.BeginFile(name, 0o644)
		require.NoError(t, err)
		_, err = b.Write([]byte("test data"))
		require.NoError(t, err)
	}
	root, err := b.Finish()
	require.NoError(t, err)
	var count int
	err = ag.ForEachLeaf(ctx, s, *root, "", func(p string, md *Info) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, count, int(N))
	t.Logf("%d files produced %d chunks", int(N), s.Len())
	require.LessOrEqual(t, s.Len(), int(N))
}

func setup(t testing.TB) (context.Context, *Machine, *schema.MemStore) {
	op := NewMachine()
	s := stores.NewMem()
	return testutil.Context(t), op, s
}
