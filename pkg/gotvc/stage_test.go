package gotvc

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/stretchr/testify/require"
)

func TestSnapshotSingleFile(t *testing.T) {
	ctx := context.Background()
	cell := cells.NewMem()
	store := cadata.NewMem()
	op := gotfs.NewOperator()
	stage := NewStage(cell, store, &op)

	makeContents := func(i int) string {
		return fmt.Sprintf("contents of file\n%d\n", i)
	}
	var base *Snapshot
	const N = 10
	for i := 0; i < N; i++ {
		err := stage.Add(ctx, "test.txt", strings.NewReader(makeContents(i)))
		require.NoError(t, err)
		base, err = stage.Snapshot(ctx, base, "", nil)
		require.NoError(t, err)
		require.Equal(t, i, int(base.N))
		requireFileContent(t, store, base.Root, "test.txt", makeContents(i))
	}
}

func requireFileContent(t *testing.T, s cadata.Store, root gotfs.Root, p, content string) {
	ctx := context.Background()
	op := gotfs.NewOperator()
	r := gotfs.NewReader(ctx, s, &op, root, p)
	data, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
}
