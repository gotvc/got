package branches

import (
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/cells"
	"github.com/stretchr/testify/require"
)

func TestMemSpace(t *testing.T) {
	TestSpace(t, newTestSpace)
}

func newTestSpace(t testing.TB) Space {
	newStore := func() cadata.Store {
		return cadata.NewMem(cadata.DefaultHash, 1<<20)
	}
	newCell := func() cells.Cell {
		return cells.NewMem()
	}
	return NewMem(newStore, newCell)
}

func TestCryptoSpace(t *testing.T) {
	TestSpace(t, func(t testing.TB) Space {
		mem := newTestSpace(t)
		secret := [32]byte{}
		return NewCryptoSpace(mem, &secret)
	})
}

func TestIsValidName(t *testing.T) {
	tcs := map[string]bool{
		"":              false,
		"test":          true,
		"test\ttest":    false,
		"test123":       true,
		"something.com": true,
		"test\n":        false,
	}
	for x, expected := range tcs {
		actual := IsValidName(x)
		require.Equal(t, expected, actual, "%s -> %v", x, actual)
	}
}

func TestPadUnpad(t *testing.T) {
	const blockSize = 16
	const letters = "abcdefghijklmnopqrstuvwxyz"
	for i := 0; i < 26; i++ {
		in := letters[:i]
		out := padBytes([]byte(in), blockSize)

		// check that the output is a multiple of blockSize
		require.Zero(t, len(out)%blockSize)
		// check that the last byte is equal to the number of added bytes
		require.Equal(t, len(out)-len(in), int(out[len(out)-1]))

		// check that it is reversible
		actual, err := unpadBytes(out, blockSize)
		require.NoError(t, err)
		require.Equal(t, string(in), string(actual))
	}
}
