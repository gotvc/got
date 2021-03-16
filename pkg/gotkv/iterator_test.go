package gotkv

import (
	"context"
	"testing"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/stretchr/testify/require"
)

func TestNextAfter(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem()
	ref, err := New(ctx, s)
	require.NoError(t, err)
	keys := []string{
		"1",
		"2",
		"3",
		"4",
	}
	for i := range keys {
		ref, err = Put(ctx, s, *ref, []byte(keys[i]), []byte("test value"))
		require.NoError(t, err)
	}
	var lastKey []byte
	for i := range keys {
		lastKey, err = NextAfter(ctx, s, *ref, lastKey)
		require.NoError(t, err)
		require.Equal(t, keys[i], string(lastKey))
	}
	_, err = NextAfter(ctx, s, *ref, lastKey)
	require.Equal(t, err, ErrNextNotFound)
}
