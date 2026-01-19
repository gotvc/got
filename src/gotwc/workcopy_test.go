package gotwc

import (
	"context"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/marks"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/posixfs"
)

func TestSetup(t *testing.T) {
	newTestWC(t, true)
}

func TestSetGetHead(t *testing.T) {
	ctx := testutil.Context(t)
	wc := newTestWC(t, true)
	name, err := wc.GetHead()
	require.NoError(t, err)
	require.Equal(t, nameMaster, name)
	require.NoError(t, wc.SetHead(ctx, nameMaster))
	name, err = wc.GetHead()
	require.NoError(t, err)
	require.Equal(t, nameMaster, name)
}

func TestEditTracking(t *testing.T) {
	ctx := testutil.Context(t)
	wc := newTestWC(t, false)
	spans, err := wc.ListSpans(ctx)
	require.NoError(t, err)
	require.Empty(t, spans)
	require.NoError(t, wc.Track(ctx, PrefixSpan("a")))
	require.NoError(t, wc.Track(ctx, PrefixSpan("b")))
	require.NoError(t, wc.Track(ctx, PrefixSpan("c")))
	spans, err = wc.ListSpans(ctx)
	require.NoError(t, err)
	require.Len(t, spans, 3)
}

func TestCommit(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	wc := newTestWC(t, true)
	fs := posixfs.NewDirFS(wc.Dir())
	p := "test.txt"
	p2 := "test2.txt"
	fileContents := "file contents\n"
	err := posixfs.PutFile(ctx, fs, p, 0o644, strings.NewReader(fileContents))
	require.NoError(t, err)
	err = wc.Put(ctx, p)
	require.NoError(t, err)

	err = wc.Commit(ctx, CommitParams{})
	require.NoError(t, err)

	checkFileContent(t, wc, p, strings.NewReader(fileContents))

	// delete p, add p2
	require.NoError(t, posixfs.DeleteFile(ctx, fs, p))
	require.NoError(t, posixfs.PutFile(ctx, fs, p2, 0o644, strings.NewReader(fileContents)))
	// track both
	require.NoError(t, wc.Put(ctx, p))
	require.NoError(t, wc.Put(ctx, p2))
	err = wc.Commit(ctx, CommitParams{})
	require.NoError(t, err)

	checkNotExists(t, wc, p)
	checkFileContent(t, wc, p2, strings.NewReader(fileContents))

	require.NoError(t, wc.repo.CheckAll(ctx))
}

func TestCommitLargeFile(t *testing.T) {
	t.Skip() // TODO
	t.Parallel()
	ctx := testutil.Context(t)
	wc := newTestWC(t, true)
	fs := posixfs.NewDirFS(wc.Dir())

	p := "largefile"
	const size = 1e9
	require.NoError(t, posixfs.PutFile(ctx, fs, p, 0o644, testutil.RandomStream(0, size)))
	require.NoError(t, wc.Put(ctx, p))
	require.NoError(t, wc.Commit(ctx, CommitParams{}))
	checkExists(t, wc, p)
	checkFileContent(t, wc, p, testutil.RandomStream(0, size))
}

func TestCommitDir(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	wc := newTestWC(t, true)
	fs := posixfs.NewDirFS(wc.Dir())

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
	require.NoError(t, wc.Put(ctx, dirpath))
	require.NoError(t, wc.Commit(ctx, CommitParams{}))
	for i := 0; i < N; i++ {
		p := getPath(i)
		content := getContent(i)
		checkExists(t, wc, p)
		checkFileContent(t, wc, p, strings.NewReader(content))
	}
}

func TestFork(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	wc := newTestWC(t, true)
	repo := wc.Repo()
	fs := posixfs.NewDirFS(wc.Dir())

	filePath := "README.md"
	const N = 10
	for i := 0; i < N; i++ {
		posixfs.PutFile(ctx, fs, filePath, 0o644, strings.NewReader("test-"+strconv.Itoa(i)))
		require.NoError(t, wc.Put(ctx, filePath))
		require.NoError(t, wc.Commit(ctx, CommitParams{}))
	}

	require.NoError(t, wc.Fork(ctx, "branch2"))
	require.NoError(t, repo.History(ctx, gotrepo.FQM{Name: "branch2"}, func(_ gotfs.Ref, _ marks.Snap) error {
		return nil
	}))
	commitCount := countCommits(t, wc.repo, "branch2")
	require.Equal(t, N, commitCount)
}

func newTestWC(t testing.TB, trackAll bool) *WC {
	r := gotrepo.NewTestRepo(t)
	_, err := r.CreateMark(context.TODO(), gotrepo.FQM{Name: nameMaster}, marks.DSConfig{}, nil)
	require.NoError(t, err)
	wcdir := t.TempDir()
	root := testutil.OpenRoot(t, wcdir)
	cfg := DefaultConfig()
	if !trackAll {
		cfg.Tracking = nil
	}
	require.NoError(t, Init(r, root, cfg))
	wc, err := New(r, root)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, wc.Close())
	})
	return wc
}

func checkFileContent(t testing.TB, wc *WC, p string, r io.Reader) {
	t.Helper()
	ctx := testutil.Context(t)
	pr, pw := io.Pipe()
	go func() {
		err := wc.repo.Cat(ctx, gotrepo.FQM{Name: getHead(t, wc)}, p, pw)
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()
	testutil.StreamsEqual(t, r, pr)
}

func getHead(t testing.TB, wc *WC) string {
	name, err := wc.GetHead()
	require.NoError(t, err)
	return name
}

func exists(t testing.TB, wc *WC, p string) bool {
	ctx := testutil.Context(t)
	var found bool
	err := wc.repo.Ls(ctx, gotrepo.FQM{Name: getHead(t, wc)}, path.Dir(p), func(ent gotfs.DirEnt) error {
		found = found || ent.Name == path.Base(p)
		return nil
	})
	require.NoError(t, err)
	return found
}

func checkExists(t testing.TB, wc *WC, p string) {
	t.Helper()
	found := exists(t, wc, p)
	require.True(t, found)
}

func checkNotExists(t testing.TB, wc *WC, p string) {
	t.Helper()
	found := exists(t, wc, p)
	require.False(t, found)
}

func countCommits(t testing.TB, repo *gotrepo.Repo, branchName string) int {
	ctx := testutil.Context(t)
	var count int
	repo.History(ctx, gotrepo.FQM{Name: branchName}, func(_ gotfs.Ref, _ marks.Snap) error {
		count++
		return nil
	})
	return count
}
