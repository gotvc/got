package reposchema

import (
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/internal/dbutil"
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
	gnsc := gotns.Client{Blobcache: bc, Machine: gotns.New(), ActAs: nil}
	require.NoError(t, gnsc.Init(ctx, *nsh, []gotns.IdentityLeaf{}))
}

func newBlobcache(t testing.TB) blobcache.Service {
	ctx := testutil.Context(t)

	db := dbutil.NewTestSQLxDB(t)
	require.NoError(t, bclocal.SetupDB(ctx, db))

	schemas := bclocal.DefaultSchemas()
	schemas[SchemaName_GotRepo] = NewSchema()
	schemas[SchemaName_GotNS] = gotns.Schema{}

	svc := bclocal.New(bclocal.Env{
		DB:      db,
		Schemas: schemas,
		Root:    GotRepoVolumeSpec(),
	})
	return svc
}
