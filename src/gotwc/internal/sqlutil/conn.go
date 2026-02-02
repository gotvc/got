package sqlutil

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Type aliases for convenience
type Pool = sqlitex.Pool
type Conn = sqlite.Conn

func OpenPool(p string) (*Pool, error) {
	// Set up connection options with WAL mode and foreign keys
	uri := "file:" + p + "?_pragma=foreign_keys(1)"

	pool, err := sqlitex.NewPool(uri, sqlitex.PoolOptions{
		PoolSize: 10, // Allow up to 10 concurrent connections
	})
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func NewTestPool(t testing.TB) *Pool {
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{
		PoolSize: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	return pool
}

// Borrow retrieves a connection from the pool and calls fn with it.
// The connection is returned to the pool after fn is called.
func Borrow(ctx context.Context, pool *Pool, fn func(conn *Conn) error) error {
	conn, err := pool.Take(ctx)
	if err != nil {
		return err
	}
	defer pool.Put(conn)
	return fn(conn)
}

// Exec executes a query without returning rows
func Exec(conn *Conn, query string, args ...any) error {
	stmt, _, err := conn.PrepareTransient(query)
	if err != nil {
		return err
	}
	defer stmt.Finalize()

	// Bind parameters
	if err := bindAll(stmt, query, args); err != nil {
		return err
	}

	if ok, err := stmt.Step(); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("dbutil.Exec: not expecting rows")
	}
	return nil
}

var ErrNoRows = errors.New("no rows found")

func IsErrNoRows(err error) bool {
	return errors.Is(err, ErrNoRows)
}

// Get retrieves a single value from a query result
func Get[T any](conn *Conn, dest *T, query string, args ...any) error {
	stmt, _, err := conn.PrepareTransient(query)
	if err != nil {
		return err
	}
	defer stmt.Finalize()

	if err := bindAll(stmt, query, args); err != nil {
		return err
	}

	hasRow, err := stmt.Step()
	if err != nil {
		return err
	}
	if !hasRow {
		return ErrNoRows
	}
	return scanValue(stmt, 0, dest)
}

type ScanFunc[T any] = func(stmt *sqlite.Stmt, dst *T) error

// GetOne runs a query that expects a single row in the result.
// (false, nil) is returned to indicate that the query was successful but there was no value.
func GetOne[T any](conn *Conn, dst *T, scan ScanFunc[T], query string, args ...any) (bool, error) {
	var n int
	for ret, err := range Select(conn, scan, query, args...) {
		if n > 0 {
			return false, fmt.Errorf("too many rows")
		}
		if err != nil {
			return false, err
		}
		*dst = ret
		n++
	}
	if n > 0 {
		return true, nil
	} else {
		return false, nil
	}
}

// Select returns an iterator over the results of the query.
// scan is called for each row.
func Select[T any](conn *Conn, scan func(stmt *sqlite.Stmt, dst *T) error, query string, args ...any) iter.Seq2[T, error] {
	newErrIter := func(err error) iter.Seq2[T, error] {
		return func(yield func(T, error) bool) {
			var zero T
			yield(zero, err)
		}
	}

	stmt, _, err := conn.PrepareTransient(query)
	if err != nil {
		return newErrIter(err)
	}
	if err := bindAll(stmt, query, args); err != nil {
		defer stmt.Finalize()
		return newErrIter(err)
	}
	return func(yield func(T, error) bool) {
		defer stmt.Finalize()
		for {
			var zero T
			hasRow, err := stmt.Step()
			if err != nil {
				yield(zero, err)
				return
			}
			if !hasRow {
				return
			}
			var val T
			if err := scan(stmt, &val); err != nil {
				return
			}
			if !yield(val, nil) {
				return
			}
		}
	}
}

func ScanInt64(stmt *sqlite.Stmt, dst *int64) error {
	*dst = stmt.ColumnInt64(0)
	return nil
}

func ScanString(stmt *sqlite.Stmt, dst *string) error {
	*dst = stmt.ColumnText(0)
	return nil
}

func DoTx(ctx context.Context, pool *Pool, fn func(conn *Conn) error) error {
	return Borrow(ctx, pool, func(conn *Conn) (retErr error) {
		defer sqlitex.Transaction(conn)(&retErr)
		return fn(conn)
	})
}

func DoTx1[T any](ctx context.Context, pool *Pool, fn func(conn *Conn) (T, error)) (T, error) {
	var ret T
	err := DoTx(ctx, pool, func(conn *Conn) error {
		var err error
		ret, err = fn(conn)
		return err
	})
	return ret, err
}

func DoTx2[T1, T2 any](ctx context.Context, pool *Pool, fn func(conn *Conn) (T1, T2, error)) (T1, T2, error) {
	var ret1 T1
	var ret2 T2
	err := DoTx(ctx, pool, func(conn *Conn) error {
		var err error
		ret1, ret2, err = fn(conn)
		return err
	})
	return ret1, ret2, err
}

// DoTxRO performs read-only transaction.
func DoTxRO(ctx context.Context, pool *Pool, fn func(conn *Conn) error) error {
	return Borrow(ctx, pool, func(conn *Conn) error {
		return fn(conn)
	})
}

func bindAll(stmt *sqlite.Stmt, q string, args []any) error {
	// Bind parameters
	if pcount := stmt.BindParamCount(); pcount != len(args) {
		return fmt.Errorf("query %q has %d params, but %d were provided", q, pcount, len(args))
	}
	for i, arg := range args {
		BindAny(stmt, i+1, arg)
	}
	return nil
}

// BindAny binds an argument to a statement.
func BindAny(stmt *sqlite.Stmt, i int, arg any) {
	switch x := arg.(type) {
	case nil:
		stmt.BindNull(i)
	case string:
		stmt.BindText(i, x)
	case bool:
		stmt.BindBool(i, x)
	case int64:
		stmt.BindInt64(i, x)
	case uint32:
		stmt.BindInt64(i, int64(x))
	case int32:
		stmt.BindInt64(i, int64(x))
	case []byte:
		if len(x) == 0 {
			x = []byte{}
		}
		stmt.BindBytes(i, x)
	case blobcache.CID:
		stmt.BindBytes(i, x[:])
	default:
		panic(arg)
	}
}

// scanValue scans a single value from a statement into dest
func scanValue[T any](stmt *sqlite.Stmt, col int, dest *T) error {
	var dest2 any = dest
	switch d := dest2.(type) {
	case *string:
		*d = stmt.ColumnText(col)
		return nil
	case *int:
		*d = stmt.ColumnInt(col)
		return nil
	case *int64:
		*d = stmt.ColumnInt64(col)
		return nil
	case *[]byte:
		*d = (*d)[:0]
		*d = append(*d, make([]byte, stmt.ColumnLen(col))...)
		if n := stmt.ColumnBytes(col, *d); n != len(*d) {
			return fmt.Errorf("scanValue: short read for []byte")
		}
		return nil
	case *bool:
		*d = stmt.ColumnInt(col) != 0
		return nil
	default:
		return fmt.Errorf("unsupported type for scanning: %T", dest)
	}
}

func scanAny[T any](stmt *sqlite.Stmt, dest *T) error {
	var dest2 any = dest
	switch x := dest2.(type) {
	case *string:
		return scanValue(stmt, 0, x)
	case *int:
		return scanValue(stmt, 0, x)
	case *int64:
		return scanValue(stmt, 0, x)
	case *[]byte:
		return scanValue(stmt, 0, x)
	case *bool:
		return scanValue(stmt, 0, x)
	default:
	}
	return fmt.Errorf("unsupported type for scanning: %T", dest)
}

// scanSlice scans multiple rows from a statement into a slice
func readIntoSlice[T any, S []T](stmt *sqlite.Stmt, dest *S) error {
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		var val T
		if err := scanAny(stmt, &val); err != nil {
			return err
		}
		*dest = append(*dest, val)
	}
}

// WALCheckpoint checkpoints the Write Ahead Log.
// It must not be called inside a transaction.
func WALCheckpoint(conn *Conn) error {
	return sqlitex.Execute(conn, "PRAGMA wal_checkpoint(TRUNCATE)", nil)
}

// Vacuum performs a full database vacuum.
// It must not be called inside a transaction.
func Vacuum(conn *Conn) error {
	return sqlitex.Execute(conn, `VACUUM`, nil)
}
