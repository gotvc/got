package dbutil

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Type aliases for convenience
type Pool = sqlitex.Pool
type Conn = sqlite.Conn

func OpenPool(p string) (*Pool, error) {
	// Set up connection options with WAL mode and foreign keys
	uri := "file:" + p + "?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"

	pool, err := sqlitex.NewPool(uri, sqlitex.PoolOptions{
		PoolSize: 10, // Allow up to 10 concurrent connections
	})
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func NewTestPool(t testing.TB) *Pool {
	pool, err := sqlitex.NewPool(":memory:", sqlitex.PoolOptions{
		PoolSize: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	return pool
}

// Get retrieves a single value from a query result
func Get(conn *Conn, dest interface{}, query string, args ...interface{}) error {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Finalize()

	// Bind parameters
	for i, arg := range args {
		switch x := arg.(type) {
		case string:
			stmt.BindText(i+1, x)
		case bool:
			stmt.BindBool(i+1, x)
		case int64:
			stmt.BindInt64(i+1, x)
		case []byte:
			stmt.BindBytes(i+1, x)
		default:
			panic(x)
		}
	}

	hasRow, err := stmt.Step()
	if err != nil {
		return err
	}
	if !hasRow {
		return fmt.Errorf("no rows found")
	}

	return scanValue(stmt, 0, dest)
}

// Select retrieves multiple rows from a query result
func Select(conn *Conn, dest interface{}, query string, args ...interface{}) error {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Finalize()

	// Bind parameters
	for i, arg := range args {
		stmt.BindBytes(i+1, []byte(fmt.Sprint(arg)))
	}

	return scanSlice(stmt, dest)
}

// Exec executes a query without returning rows
func Exec(conn *Conn, query string, args ...interface{}) error {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Finalize()

	// Bind parameters
	for i, arg := range args {
		stmt.BindBytes(i+1, []byte(fmt.Sprint(arg)))
	}

	_, err = stmt.Step()
	return err
}

func DoTx(ctx context.Context, pool *Pool, fn func(conn *Conn) error) error {
	conn := pool.Get(ctx)
	defer pool.Put(conn)

	if err := Exec(conn, "BEGIN IMMEDIATE"); err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			Exec(conn, "ROLLBACK")
			panic(r)
		}
	}()

	if err := fn(conn); err != nil {
		Exec(conn, "ROLLBACK")
		return err
	}

	return Exec(conn, "COMMIT")
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
	conn := pool.Get(ctx)
	defer pool.Put(conn)

	if err := Exec(conn, "BEGIN"); err != nil {
		return err
	}
	defer Exec(conn, "ROLLBACK") // Always rollback for read-only

	return fn(conn)
}

// scanValue scans a single value from a statement into dest
func scanValue(stmt *sqlite.Stmt, col int, dest interface{}) error {
	switch d := dest.(type) {
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
		blob := make([]byte, stmt.ColumnLen(col))
		stmt.ColumnBytes(col, blob)
		*d = make([]byte, len(blob))
		copy(*d, blob)
		return nil
	case *bool:
		*d = stmt.ColumnInt(col) != 0
		return nil
	default:
		return fmt.Errorf("unsupported type for scanning: %T", dest)
	}
}

// scanSlice scans multiple rows from a statement into a slice
func scanSlice(stmt *sqlite.Stmt, dest interface{}) error {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr || destValue.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to a slice")
	}

	sliceValue := destValue.Elem()
	sliceType := sliceValue.Type()
	elemType := sliceType.Elem()

	// Create a new slice
	newSlice := reflect.MakeSlice(sliceType, 0, 0)

	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}

		// Create new element
		elem := reflect.New(elemType).Elem()

		// For basic types, scan directly
		if elemType.Kind() == reflect.String {
			elem.SetString(stmt.ColumnText(0))
		} else if elemType.Kind() == reflect.Int64 {
			elem.SetInt(stmt.ColumnInt64(0))
		} else if elemType.Kind() == reflect.Int {
			elem.SetInt(int64(stmt.ColumnInt(0)))
		} else {
			return fmt.Errorf("unsupported slice element type: %v", elemType)
		}

		newSlice = reflect.Append(newSlice, elem)
	}
	sliceValue.Set(newSlice)
	return nil
}
