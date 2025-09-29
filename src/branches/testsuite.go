package branches

import (
	"strconv"
	"testing"

	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpace(t *testing.T, newSpace func(t testing.TB) Space) {
	t.Run("CreateGet", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		x := newSpace(t)
		b, err := x.Get(ctx, "test")
		require.ErrorIs(t, err, ErrNotExist)
		require.Nil(t, b)
		_, err = x.Create(ctx, "test", Params{})
		require.NoError(t, err)
		b, err = x.Get(ctx, "test")
		require.NoError(t, err)
		require.NotNil(t, b)
	})
	t.Run("List", func(t *testing.T) {
		//t.Parallel()
		ctx := testutil.Context(t)
		x := newSpace(t)
		const N = 20
		t.Log("creating", N, "branches")
		for i := 0; i < N; i++ {
			_, err := x.Create(ctx, "test"+strconv.Itoa(i), Params{})
			require.NoError(t, err)
		}
		t.Log("done creating branches, now listing...")
		names, err := x.List(ctx, TotalSpan(), 0)
		require.NoError(t, err)
		require.Len(t, names, N)
	})
	t.Run("Delete", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		x := newSpace(t)
		var err error
		_, err = x.Create(ctx, "test1", Params{})
		require.NoError(t, err)
		_, err = x.Create(ctx, "test2", Params{})
		require.NoError(t, err)

		_, err = x.Get(ctx, "test1")
		require.NoError(t, err)
		_, err = x.Get(ctx, "test2")
		require.NoError(t, err)

		err = x.Delete(ctx, "test1")
		require.NoError(t, err)

		_, err = x.Get(ctx, "test1")
		require.ErrorIs(t, err, ErrNotExist)
		_, err = x.Get(ctx, "test2")
		require.NoError(t, err)
	})
}
