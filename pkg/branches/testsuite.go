package branches

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRealm(t *testing.T, newRealm func(t testing.TB) Realm) {
	ctx := context.Background()
	t.Run("CreateGet", func(t *testing.T) {
		t.Parallel()
		x := newRealm(t)
		b, err := x.Get(ctx, "test")
		require.Equal(t, ErrNotExist, err)
		require.Nil(t, b)
		err = x.Create(ctx, "test")
		require.NoError(t, err)
		b, err = x.Get(ctx, "test")
		require.NoError(t, err)
		require.NotNil(t, b)
	})
}
