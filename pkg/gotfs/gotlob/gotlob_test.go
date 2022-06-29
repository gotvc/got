package gotlob

import (
	"context"
	"testing"

	"github.com/gotvc/got/pkg/stores"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	ctx := context.Background()
	op := NewOperator()
	ms, ds := stores.NewMem(), stores.NewMem()

	b := op.NewBuilder(ctx, ms, ds)
	err := b.SetPrefix([]byte("test-key"))
	require.NoError(t, err)
	_, err = b.Write([]byte("test data"))
	require.NoError(t, err)
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	t.Log(root)
}
