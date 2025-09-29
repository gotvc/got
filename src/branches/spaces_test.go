package branches

import (
	"testing"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/volumes"
	"github.com/stretchr/testify/require"
)

func TestMemSpace(t *testing.T) {
	TestSpace(t, newTestSpace)
}

func newTestSpace(t testing.TB) Space {
	newVolume := func() volumes.Volume {
		return volumes.NewMemory(gdat.Hash, 1<<22)
	}
	return NewMem(newVolume)
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
