package gotfs

import (
	"bytes"
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadDir(t *testing.T) {
	ctx, op, s := setup(t)
	x, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)
	x, err = op.Mkdir(ctx, s, *x, "dir0")
	require.NoError(t, err)
	x, err = op.Mkdir(ctx, s, *x, "dir1")
	require.NoError(t, err)
	x, err = op.Mkdir(ctx, s, *x, "dir2")
	require.NoError(t, err)
	ps := []string{"0-file1.txt", "2-file2.txt", "3-file3.txt"}
	for i := range ps {
		p := path.Join("dir1", ps[i])
		x, err = op.CreateFile(ctx, s, s, *x, p, bytes.NewReader(nil))
		require.NoError(t, err)
	}
	x, err = op.Mkdir(ctx, s, *x, "dir1/1-subdir")
	require.NoError(t, err)
	x, err = op.CreateFile(ctx, s, s, *x, "dir1/1-subdir/file.txt", bytes.NewReader(nil))
	require.NoError(t, err)

	expected := append(ps[:1], append([]string{"1-subdir"}, ps[1:]...)...)
	var i int
	err = op.ReadDir(ctx, s, *x, "dir1", func(de DirEnt) error {
		t.Log(de)
		require.Equal(t, expected[i], de.Name)
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, len(expected), i)
}

func TestMkdirAll(t *testing.T) {
	ctx, op, s := setup(t)
	x, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)
	x, err = op.MkdirAll(ctx, s, *x, "path/to/the/dir")
	require.NoError(t, err)

	requireChildren(t, op, s, *x, "", []string{"path"})
	requireChildren(t, op, s, *x, "path", []string{"to"})
	requireChildren(t, op, s, *x, "path/to", []string{"the"})
	requireChildren(t, op, s, *x, "path/to/the", []string{"dir"})
}

func requireChildren(t *testing.T, op *Operator, s Store, x Root, p string, expected []string) {
	ctx := context.Background()
	var actual []string
	err := op.ReadDir(ctx, s, x, p, func(e DirEnt) error {
		actual = append(actual, e.Name)
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, expected, actual)
}
