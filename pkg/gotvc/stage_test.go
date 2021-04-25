package gotvc

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/stretchr/testify/require"
)

func TestSnapshotSingleFile(t *testing.T) {
	ctx := context.Background()
	cell := cells.NewMem()
	s := cadata.NewMem()
	op := gotfs.NewOperator()
	stage := NewStage(cell, s, s, &op)

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
		requireFileContent(t, s, s, base.Root, "test.txt", makeContents(i))
	}
	var count int
	err := ForEachAncestor(ctx, s, *base, func(ref Ref, snap Snapshot) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, N, count)

	err = Check(ctx, s, *base, func(root gotfs.Root) error {
		return op.Check(ctx, s, root, func(gdat.Ref) error { return nil })
	})
	require.NoError(t, err)
}

func requireFileContent(t *testing.T, ms, ds cadata.Store, root gotfs.Root, p, content string) {
	ctx := context.Background()
	op := gotfs.NewOperator()
	r := op.NewReader(ctx, ms, ds, root, p)
	data, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
}
