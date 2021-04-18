package got

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepoInit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dirpath := t.TempDir()
	t.Log("testing in", dirpath)
	require.NoError(t, InitRepo(dirpath))
	repo, err := OpenRepo(dirpath)
	require.NoError(t, err)
	require.NotNil(t, repo)

	name, vol, err := repo.GetActiveVolume(ctx)
	require.NoError(t, err)
	require.Equal(t, nameMaster, name)
	require.NotNil(t, vol)
}

func TestCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dirpath := t.TempDir()
	t.Log("testing in", dirpath)
	require.NoError(t, InitRepo(dirpath))
	repo, err := OpenRepo(dirpath)
	require.NoError(t, err)
	require.NotNil(t, repo)

	p := "test.txt"

	err = ioutil.WriteFile(filepath.Join(dirpath, p), []byte("file contents\n"), 0o644)
	require.NoError(t, err)
	err = repo.Add(ctx, p)
	require.NoError(t, err)
	delta, err := repo.StagingDiff(ctx)
	require.NoError(t, err)
	additions, err := delta.ListAdditionPaths(ctx, repo.StagingStore())
	require.NoError(t, err)
	require.Contains(t, additions, p)

	err = repo.Commit(ctx, "", nil)
	require.NoError(t, err)
}
