package gotrepo

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/gotbc"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestRepoInit(t *testing.T) {
	ctx := testutil.Context(t)
	t.Parallel()
	dirpath := t.TempDir()
	t.Log("testing in", dirpath)
	bc := gotbc.NewTest(t)
	rootVol := blobcache.OID{}
	volh, err := bc.OpenFiat(ctx, rootVol, blobcache.Action_ALL)
	require.NoError(t, err)
	require.NoError(t, Init(ctx, bc, *volh, DefaultConfig()))

	repo, err := Open(ctx, bc, rootVol, nil)
	require.NoError(t, err)
	require.NotNil(t, repo)
}
