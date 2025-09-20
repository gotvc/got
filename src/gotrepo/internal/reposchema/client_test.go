package reposchema

import (
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	ctx := testutil.Context(t)
	bc := newBlobcache(t)
	client := NewClient(bc)
	nsh, err := client.Namespace(ctx)
	require.NoError(t, err)
	vh, err := client.StagingArea(ctx, new([32]byte))
	require.NoError(t, err)
	require.NotNil(t, nsh)
	require.NotNil(t, vh)

	// Initialize GotNS
	gnsc := gotns.Client{Blobcache: bc, Machine: gotns.New(), ActAs: gotns.LeafPrivate{}}
	require.NoError(t, gnsc.Init(ctx, *nsh, []gotns.IdentityLeaf{}))

	// Write some blobs to a staging area
	txn, err := blobcache.BeginTx(ctx, bc, *vh, blobcache.TxParams{Mutate: true})
	require.NoError(t, err)
	defer txn.Abort(ctx)
	var cids []blobcache.CID
	for i := 0; i < 10; i++ {
		cid, err := txn.Post(ctx, fmt.Appendf(nil, "hello %d", i))
		require.NoError(t, err)
		cids = append(cids, cid)
	}
	require.NoError(t, txn.Commit(ctx))

	// Check that the blobs exist
	txn, err = blobcache.BeginTx(ctx, bc, *vh, blobcache.TxParams{})
	require.NoError(t, err)
	defer txn.Abort(ctx)
	for _, cid := range cids {
		yes, err := txn.Exists(ctx, cid)
		require.NoError(t, err)
		require.True(t, yes)
	}
}

func newBlobcache(t testing.TB) blobcache.Service {
	env := bclocal.NewTestEnv(t)

	env.Schemas[SchemaName_GotRepo] = NewSchema()
	env.Schemas[SchemaName_GotNS] = gotns.Schema{}
	env.Root = GotRepoVolumeSpec()
	return bclocal.NewTestServiceFromEnv(t, env)
}
