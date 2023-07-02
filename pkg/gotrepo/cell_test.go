package gotrepo

import (
	"testing"

	"github.com/brendoncarroll/go-state/cells"
	"github.com/brendoncarroll/go-state/cells/celltest"
	"github.com/gotvc/got/pkg/testutil"
)

func TestCell(t *testing.T) {
	celltest.TestBytesCell(t, func(t testing.TB) cells.BytesCell {
		db := testutil.NewTestBadger(t)
		return newBadgerCell(db, []byte(t.TempDir()))
	})
}
