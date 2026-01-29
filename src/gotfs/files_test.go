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

	"github.com/gotvc/got/src/internal/stores"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestCreateFileFrom(t *testing.T) {
	ctx, ag, s := setup(t)
	x, err := ag.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	require.NotNil(t, x)
	fileData := "file contents\n"
	x, err = ag.CreateFile(ctx, [2]stores.RW{s, s}, *x, "file.txt", strings.NewReader(fileData))
	require.NoError(t, err)
	require.NotNil(t, x)
	buf := make([]byte, 128)
	n, err := ag.ReadFileAt(ctx, [2]stores.Reading{s, s}, *x, "file.txt", 0, buf)
	require.NoError(t, err)
	require.Equal(t, fileData, string(buf[:n]))
}

func TestFileInfo(t *testing.T) {
	ctx, ag, s := setup(t)
	x, err := ag.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	require.NotNil(t, x)
	x, err = ag.CreateFile(ctx, [2]stores.RW{s, s}, *x, "file.txt", bytes.NewReader(nil))
	require.NoError(t, err)
	md, err := ag.GetInfo(ctx, s, *x, "file.txt")
	require.NoError(t, err)
	require.NotNil(t, md)
	require.True(t, os.FileMode(md.Mode).IsRegular())
}

func TestLargeFiles(t *testing.T) {
	ctx, ag, s := setup(t)
	ss := [2]stores.RW{s, s}
	const N = 5
	const size = 1e8
	fileRoots := make([]Root, N)
	eg := errgroup.Group{}
	for i := 0; i < N; i++ {
		i := i
		eg.Go(func() error {
			rng := mrand.New(mrand.NewSource(int64(i)))
			x, err := ag.FileFromReader(ctx, ss, 0o755, io.LimitReader(rng, size))
			if err != nil {
				return err
			}
			fileRoots[i] = *x
			return nil
		})
	}
	require.NoError(t, eg.Wait())

	root, err := ag.NewEmpty(ctx, s, 0o755)
	require.NoError(t, err)
	for i := range fileRoots {
		root, err = ag.Graft(ctx, ss, *root, strconv.Itoa(i), fileRoots[i])
		require.NoError(t, err)
	}

	eg = errgroup.Group{}
	for i := range fileRoots {
		i := i
		p := strconv.Itoa(i)
		actualSize, err := ag.SizeOfFile(ctx, s, *root, p)
		require.NoError(t, err)
		require.Equal(t, uint64(size), actualSize)
		eg.Go(func() error {
			r, err := ag.NewReader(ctx, [2]stores.Reading{s, s}, *root, p)
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
