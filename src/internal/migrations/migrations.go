package migrations

import (
	"fmt"
	"time"

	"github.com/gotvc/got/src/internal/dbutil"
)

type Migration struct {
	RowID   int64
	Name    string
	SQLText string
}

// EnsureAll ensures all migrations have been applied.
func EnsureAll(conn *dbutil.Conn, migrations []Migration) error {
	if err := setupMigrationsTable(conn); err != nil {
		return err
	}
	for i, mig := range migrations {
		if i+1 != int(mig.RowID) {
			return fmt.Errorf("migration %s has rowid %d, expected %d", mig.Name, mig.RowID, i+1)
		}
		if err := ensure(conn, mig); err != nil {
			return err
		}
	}
	return nil
}

// ensure checks if the migration exists in the migrations table.
// The index, name and sqltext must all match exactly.
// If they don't match, an error is returned.
// Otherwise the sqlttext is applied, and a row is inserted into the migrations table.
func ensure(conn *dbutil.Conn, mig Migration) error {
	var rowID int64
	if err := dbutil.Get(conn, &rowID, "SELECT id FROM migrations WHERE id = ? AND name = ? AND sql = ?", mig.RowID, mig.Name, mig.SQLText); err != nil {
		if err.Error() == "no rows found" {
			if err := dbutil.Get(conn, &rowID, "INSERT INTO migrations (name, sql, applied_at) VALUES (?, ?, ?) RETURNING id", mig.Name, mig.SQLText, time.Now().Unix()); err != nil {
				return err
			}
			if rowID != mig.RowID {
				return fmt.Errorf("got unexpected migration rowid %d, expected %d", rowID, mig.RowID)
			}
			if err := dbutil.Exec(conn, mig.SQLText); err != nil {
				return err
			}
			return nil
		}
		return err
	}
	return nil
}

func setupMigrationsTable(conn *dbutil.Conn) error {
	return dbutil.Exec(conn, "CREATE TABLE IF NOT EXISTS migrations (id INTEGER PRIMARY KEY, name TEXT, sql TEXT, applied_at INTEGER)")
}
