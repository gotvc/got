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

func TestRenameSpace(t *testing.T) {
	ctx := testutil.Context(t)
	t.Parallel()
	bc := gotbc.NewTest(t)
	rootVol := blobcache.OID{}
	volh, err := bc.OpenFiat(ctx, rootVol, blobcache.Action_ALL)
	require.NoError(t, err)
	require.NoError(t, Init(ctx, bc, *volh, DefaultConfig()))

	repo, err := Open(ctx, bc, rootVol, nil)
	require.NoError(t, err)

	oldSpec := SpaceSpec{Blobcache: &VolumeSpec{}}

	require.NoError(t, repo.Configure(ctx, func(x Config) (Config, error) {
		x.Spaces["foo"] = oldSpec
		x.Pull = append(x.Pull, PullConfig{From: "foo", AddPrefix: "x/"})
		x.Push = append(x.Push, PushConfig{To: "foo", AddPrefix: "y/"})
		return x, nil
	}))

	require.NoError(t, repo.RenameSpace(ctx, "foo", "bar"))

	spaces, err := repo.ListSpaces(ctx)
	require.NoError(t, err)
	_, exists := spaces["foo"]
	require.False(t, exists)
	spec, exists := spaces["bar"]
	require.True(t, exists)
	require.Equal(t, oldSpec, spec)

	cfg := repo.Config()
	require.Equal(t, "bar", cfg.Pull[0].From)
	require.Equal(t, "bar", cfg.Push[0].To)
}
