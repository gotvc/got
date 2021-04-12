package gotkv

import (
	"context"
	"fmt"
	"testing"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/stretchr/testify/require"
)

func TestNewEmpty(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem()
	op := NewOperator()
	x, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)
	require.NotNil(t, x)
}

func TestPutGet(t *testing.T) {
	ctx, s, x := testSetup(t)
	op := NewOperator()
	key := []byte("key1")
	value := []byte("value")
	x, err := op.Put(ctx, s, *x, key, value)
	require.NoError(t, err)
	actualValue, err := op.Get(ctx, s, *x, key)
	require.NoError(t, err)
	require.Equal(t, value, actualValue)
}

func TestPutGetMany(t *testing.T) {
	ctx, s, x := testSetup(t)
	op := NewOperator()
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
	op := NewOperator()
	s := cadata.NewMem()
	x, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)
	return ctx, s, x
}
