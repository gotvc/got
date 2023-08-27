package gotfs

import (
	"bytes"
	"path"
	"testing"

	"github.com/gotvc/got/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestReadDir(t *testing.T) {
	ctx, ag, s := setup(t)
	x, err := ag.NewEmpty(ctx, s)
	require.NoError(t, err)
	x, err = ag.Mkdir(ctx, s, *x, "dir0")
	require.NoError(t, err)
	x, err = ag.Mkdir(ctx, s, *x, "dir1")
	require.NoError(t, err)
	x, err = ag.Mkdir(ctx, s, *x, "dir2")
	require.NoError(t, err)
	ps := []string{"0-file1.txt", "2-file2.txt", "3-file3.txt"}
	for i := range ps {
		p := path.Join("dir1", ps[i])
		x, err = ag.CreateFile(ctx, s, s, *x, p, bytes.NewReader(nil))
		require.NoError(t, err)
	}
	x, err = ag.Mkdir(ctx, s, *x, "dir1/1-subdir")
	require.NoError(t, err)
	x, err = ag.CreateFile(ctx, s, s, *x, "dir1/1-subdir/file.txt", bytes.NewReader(nil))
	require.NoError(t, err)

	expected := append(ps[:1], append([]string{"1-subdir"}, ps[1:]...)...)
	var i int
	err = ag.ReadDir(ctx, s, *x, "dir1", func(de DirEnt) error {
		t.Log(de)
		require.Equal(t, expected[i], de.Name)
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, len(expected), i)
}

func TestMkdirAll(t *testing.T) {
	ctx, ag, s := setup(t)
	x, err := ag.NewEmpty(ctx, s)
	require.NoError(t, err)
	x, err = ag.MkdirAll(ctx, s, *x, "path/to/the/dir")
	require.NoError(t, err)

	requireChildren(t, ag, s, *x, "", []string{"path"})
	requireChildren(t, ag, s, *x, "path", []string{"to"})
	requireChildren(t, ag, s, *x, "path/to", []string{"the"})
	requireChildren(t, ag, s, *x, "path/to/the", []string{"dir"})
}

func requireChildren(t *testing.T, ag *Agent, s Store, x Root, p string, expected []string) {
	ctx := testutil.Context(t)
	var actual []string
	err := ag.ReadDir(ctx, s, x, p, func(e DirEnt) error {
		actual = append(actual, e.Name)
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, expected, actual)
}
