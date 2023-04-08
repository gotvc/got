package gotkv

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv/ptree"
	"github.com/stretchr/testify/require"
)

func TestNewEmpty(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	op := newTestOperator(t)
	x, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)
	require.NotNil(t, x)
}

func TestPutGet(t *testing.T) {
	ctx, s, x := testSetup(t)
	op := newTestOperator(t)
	key := []byte("key1")
	value := []byte("value")
	x, err := op.Put(ctx, s, *x, key, value)
	require.NoError(t, err)
	ptree.DebugTree(ctx, ptree.ReadParams[Entry, Ref]{
		Store:           &ptreeGetter{op: &op.dop, s: s},
		Compare:         compareEntries,
		NewDecoder:      newDecoder,
		NewIndexDecoder: newIndexDecoder,
	}, x.toPtree(), os.Stderr)
	actualValue, err := op.Get(ctx, s, *x, key)
	require.NoError(t, err)
	require.Equal(t, value, actualValue)
}

func TestPutGetMany(t *testing.T) {
	ctx, s, x := testSetup(t)
	op := newTestOperator(t)
	const N = 200
	makeKey := func(i int) []byte {
		return []byte(fmt.Sprintf("%d-key", i))
	}
	makeValue := func(i int) []byte {
		return []byte(fmt.Sprintf("%d-value", i))
	}
	for i := 0; i < N; i++ {
		key, value := makeKey(i), makeValue(i)
		var err error
		x, err = op.Put(ctx, s, *x, key, value)
		require.NoError(t, err)
	}
	// ptree.DebugTree(s, *x)
	for i := 0; i < N; i++ {
		key, value := makeKey(i), makeValue(i)
		actualValue, err := op.Get(ctx, s, *x, key)
		require.NoError(t, err)
		require.Equal(t, string(value), string(actualValue))
	}
}

func testSetup(t *testing.T) (context.Context, cadata.Store, *Root) {
	ctx := context.Background()
	op := newTestOperator(t)
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	x, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)
	return ctx, s, x
}

func newTestOperator(t testing.TB) Operator {
	return NewOperator(1<<13, 1<<16)
}

func BenchmarkPut(b *testing.B) {
	ctx := context.Background()
	s := cadata.NewVoid(cadata.DefaultHash, cadata.DefaultMaxSize)
	op := newTestOperator(b)
	const M = 100

	bu := op.NewBuilder(s)
	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(M * (8 + 64))
	var key [8]byte
	var value [64]byte
	for i := 0; i < M*b.N; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := bu.Put(ctx, key[:], value[:]); err != nil {
			b.Fatal(err)
		}
	}
	_, err := bu.Finish(ctx)
	require.NoError(b, err)
}
