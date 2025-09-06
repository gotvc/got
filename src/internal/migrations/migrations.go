package migrations

import (
	"context"

	"github.com/gotvc/got/src/internal/dbutil"
	"go.brendoncarroll.net/exp/slices2"
	"zombiezen.com/go/sqlite/sqlitemigration"
)

type Migration struct {
	RowID   int64
	Name    string
	SQLText string
}

// EnsureAll ensures all migrations have been applied.
func EnsureAll(conn *dbutil.Conn, migrations []Migration) error {
	// Convert our Migration structs to a schema for sqlitemigration
	schema := sqlitemigration.Schema{
		Migrations: slices2.Map(migrations, func(mig Migration) string {
			return mig.SQLText
		}),
	}
	// Use sqlitemigration.Migrate to apply migrations
	ctx := context.Background()
	return sqlitemigration.Migrate(ctx, conn, schema)
}
