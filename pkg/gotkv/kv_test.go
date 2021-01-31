package gotkv

import (
	"context"
	"testing"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem()
	x, err := New(ctx, s)
	require.NoError(t, err)
	require.NotNil(t, x)
}

func TestPutGet(t *testing.T) {
	ctx, s, x := testSetup(t)
	key := []byte("key1")
	value := []byte("value")
	x, err := Put(ctx, s, *x, key, value)
	require.NoError(t, err)
	actualValue, err := Get(ctx, s, *x, key)
	require.NoError(t, err)
	require.Equal(t, value, actualValue)
}

func testSetup(t *testing.T) (context.Context, cadata.Store, *Ref) {
	ctx := context.Background()
	s := cadata.NewMem()
	x, err := New(ctx, s)
	require.NoError(t, err)
	return ctx, s, x
}
