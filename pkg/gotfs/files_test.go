package gotfs

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/stretchr/testify/require"
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
	n, err := op.ReadFileAt(ctx, s, *x, "file.txt", 0, buf)
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
