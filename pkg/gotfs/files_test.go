package gotfs

import (
	"bytes"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestCreateFileFrom(t *testing.T) {
	ctx, op, s := setup(t)
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

func TestFileInfo(t *testing.T) {
	ctx, op, s := setup(t)
	x, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)
	require.NotNil(t, x)
	x, err = op.CreateFile(ctx, s, s, *x, "file.txt", bytes.NewReader(nil))
	require.NoError(t, err)
	md, err := op.GetInfo(ctx, s, *x, "file.txt")
	require.NoError(t, err)
	require.NotNil(t, md)
	require.True(t, os.FileMode(md.Mode).IsRegular())
}

func TestLargeFiles(t *testing.T) {
	ctx, op, s := setup(t)
	const N = 5
	const size = 1e8
	fileRoots := make([]Root, N)
	eg := errgroup.Group{}
	for i := 0; i < N; i++ {
		i := i
		eg.Go(func() error {
			rng := mrand.New(mrand.NewSource(int64(i)))
			x, err := op.FileFromReader(ctx, s, s, 0o755, io.LimitReader(rng, size))
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
		root, err = op.Graft(ctx, s, s, *root, strconv.Itoa(i), fileRoots[i])
		require.NoError(t, err)
	}

	eg = errgroup.Group{}
	for i := range fileRoots {
		i := i
		p := strconv.Itoa(i)
		actualSize, err := op.SizeOfFile(ctx, s, *root, p)
		require.NoError(t, err)
		require.Equal(t, uint64(size), actualSize)
		eg.Go(func() error {
			r, err := op.NewReader(ctx, s, s, *root, p)
			require.NoError(t, err)
			n, err := io.Copy(io.Discard, r)
			if err != nil {
				return err
			}
			if n != size {
				return fmt.Errorf("reader returned wrong number of bytes HAVE: %d WANT: %d", n, uint64(size))
			}
			return nil
		})
	}
	require.NoError(t, eg.Wait())
}
