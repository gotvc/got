package got

import (
	"bytes"
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
	fileContents := "file contents\n"
	err = ioutil.WriteFile(filepath.Join(dirpath, p), []byte(fileContents), 0o644)
	require.NoError(t, err)
	err = repo.Track(ctx, p)
	require.NoError(t, err)

	err = repo.Commit(ctx, "", nil)
	require.NoError(t, err)

	buf := bytes.Buffer{}
	err = repo.Cat(ctx, p, &buf)
	require.NoError(t, err)
	require.Equal(t, fileContents, buf.String())
}
