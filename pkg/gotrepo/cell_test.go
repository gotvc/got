package gotrepo

import (
	"testing"

	"github.com/gotvc/got/pkg/testutil"
	"go.brendoncarroll.net/state/cells"
	"go.brendoncarroll.net/state/cells/celltest"
)

func TestCell(t *testing.T) {
	celltest.TestBytesCell(t, func(t testing.TB) cells.BytesCell {
		db := testutil.NewTestBadger(t)
		return newBadgerCell(db, []byte(t.TempDir()))
	})
}
