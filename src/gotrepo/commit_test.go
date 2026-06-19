package gotrepo

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/gotbc"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpace(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	gotcore.TestSpace(t, func(t testing.TB) gotcore.Space {
		bc := gotbc.NewTest(t)
		rootVol := blobcache.OID{}
		volh, err := bc.OpenFiat(ctx, rootVol, blobcache.Action_ALL)
		require.NoError(t, err)
		require.NoError(t, Init(ctx, bc, *volh, DefaultConfig()))
		r, err := Open(ctx, bc, rootVol, nil)
		require.NoError(t, err)
		require.NotNil(t, r)
		// have to delete the automatically created master branch to get a clean slate,
		// which is what the test expects.
		space, err := r.GetSpace(ctx, "")
		require.NoError(t, err)
		return space
	})
}
