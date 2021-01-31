package gotfs

import (
	"bytes"
	"context"
	"testing"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/stretchr/testify/require"
)

func TestCreateFileFrom(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem()
	x, err := New(ctx, s)
	require.NoError(t, err)
	require.NotNil(t, x)
	fileData := []byte("file contents\n")
	x, err = CreateFileFrom(ctx, s, *x, "file.txt", bytes.NewReader(fileData))
	require.NoError(t, err)
	require.NotNil(t, x)
}
