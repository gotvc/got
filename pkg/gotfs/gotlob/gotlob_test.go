package gotlob

import (
	"context"
	"fmt"
	"io"
	mrand "math/rand"
	"testing"

	"github.com/gotvc/got/pkg/stores"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	ctx := context.Background()
	op := NewOperator()
	ms, ds := stores.NewMem(), stores.NewMem()

	b := op.NewBuilder(ctx, ms, ds)
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key-%04d", i)
		b.Put(ctx, []byte(k), []byte("value"))
		err := b.SetPrefix([]byte(k + "-data"))
		require.NoError(t, err)
		rng := mrand.New(mrand.NewSource(int64(i)))
		_, err = io.CopyN(b, rng, 10e6)
		require.NoError(t, err)
	}
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	t.Log(root)
}
