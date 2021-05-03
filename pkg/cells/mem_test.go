package cells

import (
	"testing"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/c/cellutil"
)

func TestMem(t *testing.T) {
	cellutil.CellTestSuite(t, func() p2p.Cell {
		return NewMem()
	})
}
