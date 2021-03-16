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
	x, err := New(ctx, s)
	require.NoError(t, err)
	x, err = Mkdir(ctx, s, *x, "dir1")
	require.NoError(t, err)
	ps := []string{"file1.txt", "file2.txt", "file3.txt"}
	for i := range ps {
		p := path.Join("dir1", ps[i])
		x, err = CreateFileFrom(ctx, s, *x, p, bytes.NewReader(nil))
		require.NoError(t, err)
	}
	var i int
	err = ReadDir(ctx, s, *x, "dir1", func(de DirEnt) error {
		t.Log(de)
		require.Equal(t, ps[i], de.Name)
		i++
		return nil
	})
	require.NoError(t, err)
}
