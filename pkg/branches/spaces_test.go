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
		return cadata.NewMem(1 << 20)
	}
	newCell := func() cells.Cell {
		return cells.NewMem()
	}
	return NewMem(newStore, newCell)
}

func TestCryptoSpace(t *testing.T) {
	TestSpace(t, func(t testing.TB) Space {
		mem := newTestSpace(t)
		secret := make([]byte, 32)
		return NewCryptoSpace(mem, secret)
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