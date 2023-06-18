package testutil

import (
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
)

func NewTestBadger(t testing.TB) *badger.DB {
	dbPath := filepath.Join(t.TempDir(), "badger.db")
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}
