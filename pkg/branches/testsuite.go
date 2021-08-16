package branches

import (
	"context"
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
		_, err = x.Create(ctx, "test")
		require.NoError(t, err)
		b, err = x.Get(ctx, "test")
		require.NoError(t, err)
		require.NotNil(t, b)
	})
}
