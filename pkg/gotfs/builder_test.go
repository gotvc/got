package gotfs

import (
	"context"
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
)

func TestBuilderMkdir(t *testing.T) {
	ctx, op, s := setup(t)
	b := op.NewBuilder(ctx, s, s)
	require.Error(t, b.Mkdir("1", 0o755))
	require.NoError(t, b.Mkdir("", 0o755))
	var p string
	for i := 1; i < 10; i++ {
		p += "/" + strconv.Itoa(i)
		require.NoError(t, b.Mkdir(p, 0o755))
	}
}

func setup(t testing.TB) (context.Context, Operator, cadata.Store) {
	op := NewOperator()
	s := cadata.NewMem(cadata.DefaultHash, DefaultMaxBlobSize)
	return context.Background(), op, s
}
