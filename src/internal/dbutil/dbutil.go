package dbutil

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func OpenDB(p string) (*sqlx.DB, error) {
	// How To for PRAGMAs with the modernc.org/sqlite driver
	// https://pkg.go.dev/modernc.org/sqlite@v1.34.4#Driver.Open
	db, err := sqlx.Open("sqlite", "file:"+p+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func DoTx(ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func DoTx1[T any](ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) (T, error)) (T, error) {
	var ret T
	err := DoTx(ctx, db, func(tx *sqlx.Tx) error {
		var err error
		ret, err = fn(tx)
		return err
	})
	return ret, err
}

// DoTxRO is performs read-only transaction.
func DoTxRO(ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(tx)
}

func NewTestDB(t testing.TB) *sqlx.DB {
	db, err := sqlx.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}
