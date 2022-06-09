package branches

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpace(t *testing.T, newSpace func(t testing.TB) Space) {
	ctx := context.Background()
	t.Run("CreateGet", func(t *testing.T) {
		t.Parallel()
		x := newSpace(t)
		b, err := x.Get(ctx, "test")
		require.Equal(t, ErrNotExist, err)
		require.Nil(t, b)
		_, err = x.Create(ctx, "test", Params{})
		require.NoError(t, err)
		b, err = x.Get(ctx, "test")
		require.NoError(t, err)
		require.NotNil(t, b)
	})
	t.Run("List", func(t *testing.T) {
		t.Parallel()
		x := newSpace(t)
		const N = 100
		for i := 0; i < N; i++ {
			_, err := x.Create(ctx, "test"+strconv.Itoa(i), Params{})
			require.NoError(t, err)
		}
		names, err := x.List(ctx, TotalSpan(), 0)
		require.NoError(t, err)
		require.Len(t, names, N)
	})
}
