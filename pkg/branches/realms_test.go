package branches

import (
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
)

func TestMemRealm(t *testing.T) {
	TestRealm(t, newTestRealm)
}

func newTestRealm(t testing.TB) Realm {
	newStore := func() cadata.Store {
		return cadata.NewMem(1 << 20)
	}
	newCell := func() cells.Cell {
		return cells.NewMem()
	}
	return NewMem(newStore, newCell)
}

func TestCryptoRealm(t *testing.T) {
	TestRealm(t, func(t testing.TB) Realm {
		mem := newTestRealm(t)
		secret := make([]byte, 32)
		return NewCryptoRealm(mem, secret)
	})
}
