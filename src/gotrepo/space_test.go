package gotrepo

import (
	"testing"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpace(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	branches.TestSpace(t, func(t testing.TB) branches.Space {
		dir := t.TempDir()
		err := Init(dir, DefaultConfig())
		require.NoError(t, err)
		r, err := Open(dir)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})
		// have to delete the automatically created master branch to get a clean slate,
		// which is what the test expects.
		space, err := r.GetSpace(ctx)
		require.NoError(t, err)
		require.NoError(t, space.Delete(ctx, nameMaster))
		return space
	})
}
