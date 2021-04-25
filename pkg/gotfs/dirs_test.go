package gotfs

import (
	"bytes"
	"context"
	"path"
	"testing"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/stretchr/testify/require"
)

func TestReadDir(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem()
	op := NewOperator()
	x, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)
	x, err = op.Mkdir(ctx, s, *x, "dir0")
	require.NoError(t, err)
	x, err = op.Mkdir(ctx, s, *x, "dir1")
	require.NoError(t, err)
	x, err = op.Mkdir(ctx, s, *x, "dir2")
	require.NoError(t, err)
	ps := []string{"file1.txt", "file2.txt", "file3.txt"}
	for i := range ps {
		p := path.Join("dir1", ps[i])
		x, err = op.CreateFile(ctx, s, s, *x, p, bytes.NewReader(nil))
		require.NoError(t, err)
	}
	x, err = op.Mkdir(ctx, s, *x, "dir1/subdir")
	require.NoError(t, err)
	x, err = op.CreateFile(ctx, s, s, *x, "dir1/subdir/file.txt", bytes.NewReader(nil))
	require.NoError(t, err)
	ps = append(ps, "subdir")
	var i int
	err = op.ReadDir(ctx, s, *x, "dir1", func(de DirEnt) error {
		t.Log(de)
		require.Equal(t, ps[i], de.Name)
		i++
		return nil
	})
	require.NoError(t, err)
}
