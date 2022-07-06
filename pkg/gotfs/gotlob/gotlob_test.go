package gotlob

import (
	"context"
	"fmt"
	"io"
	mrand "math/rand"
	"strconv"
	"testing"

	"github.com/gotvc/got/pkg/gotkv"
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

func TestSetPrefix(t *testing.T) {
	ctx := context.Background()
	op := NewOperator()
	ms, ds := stores.NewMem(), stores.NewMem()
	b := op.NewBuilder(ctx, ms, ds)

	err := b.SetPrefix([]byte("prefix1"))
	require.NoError(t, err)
	err = b.SetPrefix([]byte("prefix1"))
	require.Error(t, err)
	err = b.SetPrefix([]byte("prefix2"))
	require.NoError(t, err)
}

func TestCopy(t *testing.T) {
	ctx := context.Background()
	op := NewOperator()
	ms, ds := stores.NewMem(), stores.NewMem()
	roots := make([]Root, 3)
	for i := range roots {
		b := op.NewBuilder(ctx, ms, ds)
		prefix := strconv.Itoa(i) + "\x00"
		b.SetPrefix([]byte(prefix))
		rng := randomStream(i, 1e8)
		_, err := io.Copy(b, rng)
		require.NoError(t, err)
		root, err := b.Finish(ctx)
		require.NoError(t, err)
		roots[i] = *root
	}

	b := op.NewBuilder(ctx, ms, ds)
	for i := range roots {
		prefix := strconv.Itoa(i) + "\x00"
		err := b.CopyFrom(ctx, roots[i], gotkv.PrefixSpan([]byte(prefix)))
		require.NoError(t, err)
	}
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	t.Log(root)
}

func randomStream(seed int, size int64) io.Reader {
	return io.LimitReader(mrand.New(mrand.NewSource(int64(seed))), size)
}
