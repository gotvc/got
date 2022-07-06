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
	"github.com/gotvc/got/pkg/testutil"
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
	op := NewOperator(WithFilter(func(x []byte) bool {
		return len(x) >= 9
	}))
	ms, ds := stores.NewMem(), stores.NewMem()
	roots := make([]Root, 3)
	for i := range roots {
		b := op.NewBuilder(ctx, ms, ds)
		err := b.Put(ctx, []byte(strconv.Itoa(i)), []byte("test-value"))
		require.NoError(t, err)
		prefix := strconv.Itoa(i) + "\x00"
		b.SetPrefix([]byte(prefix))
		rng := testutil.RandomStream(i, 1e8)
		_, err = io.Copy(b, rng)
		require.NoError(t, err)
		root, err := b.Finish(ctx)
		require.NoError(t, err)
		roots[i] = *root
	}

	b := op.NewBuilder(ctx, ms, ds)
	for i := range roots {
		err := b.CopyFrom(ctx, roots[i], gotkv.TotalSpan())
		require.NoError(t, err)
	}
	root, err := b.Finish(ctx)
	require.NoError(t, err)

	for i := range roots {
		prefix := strconv.Itoa(i) + "\x00"

		r, err := op.NewReader(ctx, ms, ds, *root, []byte(prefix))
		require.NoError(t, err)
		rng := testutil.RandomStream(i, 1e8)
		testutil.StreamsEqual(t, rng, r)

		k := strconv.Itoa(i)
		v, err := gotkv.Get(ctx, ms, *root, []byte(k))
		require.NoError(t, err, "%v", k)
		require.Equal(t, "test-value", string(v))
	}
}
