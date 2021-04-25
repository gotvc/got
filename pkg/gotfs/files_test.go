package gotfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestCreateFileFrom(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem()
	op := NewOperator()
	x, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)
	require.NotNil(t, x)
	fileData := "file contents\n"
	x, err = op.CreateFile(ctx, s, s, *x, "file.txt", strings.NewReader(fileData))
	require.NoError(t, err)
	require.NotNil(t, x)
	buf := make([]byte, 128)
	n, err := op.ReadFileAt(ctx, s, s, *x, "file.txt", 0, buf)
	require.NoError(t, err)
	require.Equal(t, fileData, string(buf[:n]))
}

func TestFileMetadata(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem()
	op := NewOperator()
	x, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)
	require.NotNil(t, x)
	x, err = op.CreateFile(ctx, s, s, *x, "file.txt", bytes.NewReader(nil))
	require.NoError(t, err)
	md, err := op.GetMetadata(ctx, s, *x, "file.txt")
	require.NoError(t, err)
	require.NotNil(t, md)
	require.True(t, os.FileMode(md.Mode).IsRegular())
}

func TestLargeFiles(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem()
	op := NewOperator()

	const N = 5
	fileRoots := make([]Root, N)
	eg := errgroup.Group{}
	for i := 0; i < N; i++ {
		i := i
		eg.Go(func() error {
			x, err := op.CreateFileRoot(ctx, s, s, io.LimitReader(rand.Reader, 1e8))
			if err != nil {
				return err
			}
			fileRoots[i] = *x
			return nil
		})
	}
	require.NoError(t, eg.Wait())

	root, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)
	for i := range fileRoots {
		root, err = op.Graft(ctx, s, *root, strconv.Itoa(i), fileRoots[i])
		require.NoError(t, err)
	}
}
