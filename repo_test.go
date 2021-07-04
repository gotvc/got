package got

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
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

	name, branch, err := repo.GetActiveBranch(ctx)
	require.NoError(t, err)
	require.Equal(t, nameMaster, name)
	require.NotNil(t, branch)
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
	p2 := "test2.txt"
	fileContents := "file contents\n"
	err = ioutil.WriteFile(filepath.Join(dirpath, p), []byte(fileContents), 0o644)
	require.NoError(t, err)
	err = repo.Track(ctx, p)
	require.NoError(t, err)

	err = repo.Commit(ctx, CommitInfo{})
	require.NoError(t, err)

	checkFileContent(t, repo, p, fileContents)

	// delete p, add p2
	require.NoError(t, os.Remove(filepath.Join(dirpath, p)))
	require.NoError(t, ioutil.WriteFile(filepath.Join(dirpath, p2), []byte(fileContents), 0o644))
	// track both
	require.NoError(t, repo.Track(ctx, p))
	require.NoError(t, repo.Track(ctx, p2))
	err = repo.Commit(ctx, CommitInfo{})
	require.NoError(t, err)

	checkNotExists(t, repo, p)
	checkFileContent(t, repo, p2, fileContents)

	require.NoError(t, repo.Check(ctx))
}

func checkFileContent(t testing.TB, repo *Repo, p, content string) {
	ctx := context.Background()
	buf := bytes.Buffer{}
	err := repo.Cat(ctx, p, &buf)
	require.NoError(t, err)
	require.Equal(t, content, buf.String())
}

func checkNotExists(t testing.TB, repo *Repo, p string) {
	ctx := context.Background()
	err := repo.Cat(ctx, p, io.Discard)
	require.True(t, os.IsNotExist(err))
}
