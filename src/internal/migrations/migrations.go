package migrations

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type Migration struct {
	RowID   int64
	Name    string
	SQLText string
}

// EnsureAll ensures all migrations have been applied.
func EnsureAll(tx *sqlx.Tx, migrations []Migration) error {
	if err := setupMigrationsTable(tx); err != nil {
		return err
	}
	for i, mig := range migrations {
		if i+1 != int(mig.RowID) {
			return fmt.Errorf("migration %s has rowid %d, expected %d", mig.Name, mig.RowID, i+1)
		}
		if err := ensure(tx, mig); err != nil {
			return err
		}
	}
	return nil
}

// ensure checks if the migration exists in the migrations table.
// The index, name and sqltext must all match exactly.
// If they don't match, an error is returned.
// Otherwise the sqlttext is applied, and a row is inserted into the migrations table.
func ensure(tx *sqlx.Tx, mig Migration) error {
	var rowID int64
	if err := tx.Get(&rowID, "SELECT id FROM migrations WHERE id = ? AND name = ? AND sql = ?", mig.RowID, mig.Name, mig.SQLText); err != nil {
		if err == sql.ErrNoRows {
			if err := tx.Get(&rowID, "INSERT INTO migrations (name, sql, applied_at) VALUES (?, ?, ?) RETURNING id", mig.Name, mig.SQLText, time.Now().Unix()); err != nil {
				return err
			}
			if rowID != mig.RowID {
				return fmt.Errorf("got unexpected migration rowid %d, expected %d", rowID, mig.RowID)
			}
			_, err := tx.Exec(mig.SQLText)
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}
	return nil
}

func setupMigrationsTable(tx *sqlx.Tx) error {
	_, err := tx.Exec("CREATE TABLE IF NOT EXISTS migrations (id INTEGER PRIMARY KEY, name TEXT, sql TEXT, applied_at INTEGER)")
	return err
}
