package gotrepo

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/sync/errgroup"
)

func TestRepoInit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dirpath := t.TempDir()
	t.Log("testing in", dirpath)
	require.NoError(t, Init(dirpath))
	repo, err := Open(dirpath)
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
	repo := newTestRepo(t)
	fs := repo.WorkingDir()
	p := "test.txt"
	p2 := "test2.txt"
	fileContents := "file contents\n"
	err := posixfs.PutFile(ctx, fs, p, 00644, strings.NewReader(fileContents))
	require.NoError(t, err)
	err = repo.Put(ctx, p)
	require.NoError(t, err)

	err = repo.Commit(ctx, gotvc.SnapInfo{})
	require.NoError(t, err)

	checkFileContent(t, repo, p, strings.NewReader(fileContents))

	// delete p, add p2
	require.NoError(t, posixfs.DeleteFile(ctx, fs, p))
	require.NoError(t, posixfs.PutFile(ctx, fs, p2, 0o644, strings.NewReader(fileContents)))
	// track both
	require.NoError(t, repo.Put(ctx, p))
	require.NoError(t, repo.Put(ctx, p2))
	err = repo.Commit(ctx, gotvc.SnapInfo{})
	require.NoError(t, err)

	checkNotExists(t, repo, p)
	checkFileContent(t, repo, p2, strings.NewReader(fileContents))

	require.NoError(t, repo.Check(ctx))
}

func TestCommitLargeFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := newTestRepo(t)
	fs := repo.WorkingDir()

	p := "largefile"
	const size = 500e6
	newReader := func() io.Reader {
		rng := rand.New(rand.NewSource(0))
		rand.New(rand.NewSource(0))
		return io.LimitReader(rng, size)
	}
	require.NoError(t, posixfs.PutFile(ctx, fs, p, 0o644, newReader()))
	require.NoError(t, repo.Put(ctx, p))
	require.NoError(t, repo.Commit(ctx, gotvc.SnapInfo{}))
	checkExists(t, repo, p)
	checkFileContent(t, repo, p, newReader())
}

func TestCommitDir(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := newTestRepo(t)
	fs := repo.WorkingDir()

	dirpath := "path/to/dir"
	require.NoError(t, posixfs.MkdirAll(fs, dirpath, 0o755))
	const N = 10
	getPath := func(i int) string {
		return path.Join(dirpath, strconv.Itoa(i))
	}
	getContent := func(i int) string {
		return fmt.Sprintf("file data %016x", i)
	}
	for i := 0; i < N; i++ {
		p := getPath(i)
		content := getContent(i)
		require.NoError(t, posixfs.PutFile(ctx, fs, p, 0o644, strings.NewReader(content)))
	}
	require.NoError(t, repo.Put(ctx, dirpath))
	require.NoError(t, repo.Commit(ctx, gotvc.SnapInfo{}))
	for i := 0; i < N; i++ {
		p := getPath(i)
		content := getContent(i)
		checkExists(t, repo, p)
		checkFileContent(t, repo, p, strings.NewReader(content))
	}
}

func TestFork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := newTestRepo(t)
	fs := repo.WorkingDir()

	filePath := "README.md"
	const N = 10
	for i := 0; i < N; i++ {
		posixfs.PutFile(ctx, fs, filePath, 0o644, strings.NewReader("test-"+strconv.Itoa(i)))
		require.NoError(t, repo.Put(ctx, filePath))
		require.NoError(t, repo.Commit(ctx, gotvc.SnapInfo{}))
	}

	require.NoError(t, repo.Fork(ctx, "", "branch2"))
	require.NoError(t, repo.History(ctx, "branch2", func(_ Ref, _ Snap) error {
		return nil
	}))
	commitCount := countCommits(t, repo, "branch2")
	require.Equal(t, N, commitCount)
}

func newTestRepo(t testing.TB) *Repo {
	dirpath := t.TempDir()
	t.Log("testing in", dirpath)
	require.NoError(t, Init(dirpath))
	repo, err := Open(dirpath)
	require.NoError(t, err)
	require.NotNil(t, repo)
	return repo
}

func checkFileContent(t testing.TB, repo *Repo, p string, r io.Reader) {
	ctx := context.Background()
	var expected, actual [32]byte
	eg := errgroup.Group{}
	eg.Go(func() error {
		h, err := blake2b.New256(nil)
		if err != nil {
			panic(err)
		}
		if _, err := io.Copy(h, r); err != nil {
			return err
		}
		h.Sum(expected[:0])
		return nil
	})
	eg.Go(func() error {
		h, err := blake2b.New256(nil)
		if err != nil {
			panic(err)
		}
		if err := repo.Cat(ctx, p, h); err != nil {
			return err
		}
		h.Sum(actual[:0])
		return nil
	})
	require.NoError(t, eg.Wait())
	require.Equal(t, expected, actual, "file %q content did not match", p)
}

func exists(t testing.TB, repo *Repo, p string) bool {
	ctx := context.Background()
	var found bool
	err := repo.Ls(ctx, path.Dir(p), func(ent gotfs.DirEnt) error {
		found = found || ent.Name == path.Base(p)
		return nil
	})
	require.NoError(t, err)
	return found
}

func checkExists(t testing.TB, repo *Repo, p string) {
	t.Helper()
	found := exists(t, repo, p)
	require.True(t, found)
}

func checkNotExists(t testing.TB, repo *Repo, p string) {
	t.Helper()
	found := exists(t, repo, p)
	require.False(t, found)
}

func countCommits(t testing.TB, repo *Repo, branchName string) int {
	ctx := context.Background()
	var count int
	repo.History(ctx, branchName, func(_ Ref, _ Snap) error {
		count++
		return nil
	})
	return count
}
