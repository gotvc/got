package dbutil

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// OpenSQLxDB opens a database using the old sqlx interface (for backward compatibility)
func OpenSQLxDB(p string) (*sqlx.DB, error) {
	// How To for PRAGMAs with the modernc.org/sqlite driver
	// https://pkg.go.dev/modernc.org/sqlite@v1.34.4#Driver.Open
	db, err := sqlx.Open("sqlite", "file:"+p+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func NewTestSQLxDB(t testing.TB) *sqlx.DB {
	db, err := sqlx.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}
