package gotlob

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slog"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/stores"
	"github.com/gotvc/got/pkg/testutil"
)

var ctx = func() context.Context {
	l := slog.New(slog.NewTextHandler(os.Stderr))
	return logctx.NewContext(context.Background(), &l)
}()

func TestWrite(t *testing.T) {
	t.Parallel()
	op := newOperator(t)
	ms, ds := stores.NewMem(), stores.NewMem()
	const N = 10
	const size = 10e6

	b := op.NewBuilder(ctx, ms, ds)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key-%04d", i)
		b.Put(ctx, []byte(k), []byte("value"))
		prefix := []byte(k + "-data")
		err := b.SetPrefix(prefix)
		require.NoError(t, err)
		rng := testutil.RandomStream(i, size)
		_, err = io.Copy(b, rng)
		require.NoError(t, err)
	}
	root, err := b.Finish(ctx)
	require.NoError(t, err)
	for i := 0; i < N; i++ {
		prefix := fmt.Sprintf("key-%04d-data", i)
		r, err := op.NewReader(ctx, ms, ds, *root, []byte(prefix))
		require.NoError(t, err)
		t.Logf("reading prefix %q", prefix)
		testutil.StreamsEqual(t, testutil.RandomStream(i, size), r)
	}
}

func TestSetPrefix(t *testing.T) {
	t.Parallel()
	op := newOperator(t)
	ms, ds := stores.NewMem(), stores.NewMem()
	b := op.NewBuilder(ctx, ms, ds)

	err := b.SetPrefix([]byte("prefix1"))
	require.NoError(t, err)
	err = b.SetPrefix([]byte("prefix1"))
	require.Error(t, err)
	err = b.SetPrefix([]byte("prefix2"))
	require.NoError(t, err)
}

func TestCopyFrom(t *testing.T) {
	t.Parallel()
	op := newOperator(t, WithFilter(func(x []byte) bool {
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

func TestCopyExtents(t *testing.T) {
	t.Parallel()
	op := newOperator(t)
	ms, ds := stores.NewMem(), stores.NewMem()
	b := op.NewBuilder(ctx, ms, ds)

	const N = 5
	var exts [][]*Extent
	for i := 0; i < N; i++ {
		exts2, err := op.CreateExtents(ctx, ds, testutil.RandomStream(i, 10e6))
		require.NoError(t, err)
		exts = append(exts, exts2)
	}
	err := b.SetPrefix([]byte("0"))
	require.NoError(t, err)
	for i := range exts {
		err := b.CopyExtents(ctx, exts[i])
		require.NoError(t, err)
	}
	root, err := b.Finish(ctx)
	require.NoError(t, err)

	rngs := make([]io.Reader, N)
	for i := range rngs {
		rngs[i] = testutil.RandomStream(i, 10e6)
	}
	actual, err := op.NewReader(ctx, ms, ds, *root, []byte("0"))
	require.NoError(t, err)
	expected := io.MultiReader(rngs...)
	testutil.StreamsEqual(t, expected, actual)
}

func newOperator(t testing.TB, opts ...Option) Operator {
	gkv := gotkv.NewOperator(1<<13, 1<<20)
	dop := gdat.NewOperator()
	o := NewOperator(&gkv, &dop, opts...)
	return o
}
