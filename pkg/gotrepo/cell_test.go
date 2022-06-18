package gotrepo

import (
	"path/filepath"
	"testing"

	"github.com/brendoncarroll/go-state/cells"
	"github.com/brendoncarroll/go-state/cells/celltest"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestBoltCell(t *testing.T) {
	celltest.CellTestSuite(t, func(t testing.TB) cells.Cell {
		dbPath := filepath.Join(t.TempDir(), "bolt.db")
		db, err := bolt.Open(dbPath, 0o644, &bolt.Options{})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})
		return newBoltCell(db, []string{"test-cells"}, []byte("key1"))
	})
}
