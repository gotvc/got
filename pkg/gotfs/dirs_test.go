package gotfs

import (
	"bytes"
	"context"
	"path"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
)

func TestReadDir(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem(DefaultMaxBlobSize)
	op := NewOperator()
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
