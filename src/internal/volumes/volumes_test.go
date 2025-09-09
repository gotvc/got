package volumes

import (
	"testing"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestChaCha20Poly1305(t *testing.T) {
	ctx := testutil.Context(t)
	inner := NewMemory(gdat.Hash, 1024)
	secret := [32]byte{}
	vol := NewChaCha20Poly1305(inner, &secret)

	tx, err := vol.BeginTx(ctx, TxParams{Mutate: true})
	require.NoError(t, err)

	var empty []byte
	require.NoError(t, tx.Load(ctx, &empty))
	require.Equal(t, empty, []byte{})

	x := []byte("hello")
	require.NoError(t, tx.Save(ctx, x))
	var y []byte
	require.NoError(t, tx.Load(ctx, &y))
	require.Equal(t, x, y)
}
