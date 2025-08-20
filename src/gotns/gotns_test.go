package gotns

import (
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/simplens"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	ctx := testutil.Context(t)
	bc := bclocal.NewTestService(t)
	nsc := simplens.Client{Service: bc}
	volh, err := nsc.CreateAt(ctx, blobcache.Handle{}, "test", blobcache.DefaultLocalSpec())
	require.NoError(t, err)

	gnsc := Client{Blobcache: bc, Machine: New()}
	err = gnsc.Init(ctx, *volh)
	require.NoError(t, err)
}
