package branches

import (
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"

	"github.com/gotvc/got/pkg/cells"
	"github.com/gotvc/got/pkg/stores"
)

func TestMemSpace(t *testing.T) {
	TestSpace(t, newTestSpace)
}

func newTestSpace(t testing.TB) Space {
	newStore := func() cadata.Store {
		return stores.NewMem()
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

func TestCheckName(t *testing.T) {
	tcs := map[string]bool{
		"":              false,
		"test":          true,
		"test\ttest":    false, // no \t
		"test123":       true,
		"something.com": true,  // .
		"test\n":        false, // no \n
		"test/-_":       true,  // / - _
		"test ":         false, // no spaces
	}
	for x, expected := range tcs {
		actual := CheckName(x)
		if !expected {
			require.Error(t, actual, "%s -> %v", x, actual)
		} else {
			require.NoError(t, actual, "%s -> %v", x, actual)
		}
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
