package gotwc

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/gotvc/got/src/gotwc/internal/sqlutil"
	"github.com/gotvc/got/src/gotwc/internal/staging"
	"github.com/gotvc/got/src/internal/marks"
	"go.brendoncarroll.net/state"
	"zombiezen.com/go/sqlite"
)

// createStagingArea creates a new staging area and returns its id.
func createStagingArea(conn *sqlutil.Conn, parahHash *[32]byte) (int64, error) {
	var rowid int64
	err := sqlutil.Get(conn, &rowid, `INSERT INTO staging_areas (param_hash) VALUES (?) RETURNING rowid`, parahHash[:])
	if err != nil {
		return 0, err
	}
	return rowid, err
}

// ensureStagingArea finds the staging area with the given salt, or creates a new one if it doesn't exist.
func ensureStagingArea(conn *sqlutil.Conn, parahHash *[32]byte) (int64, error) {
	var id int64
	err := sqlutil.Get(conn, &id, `SELECT rowid FROM staging_areas WHERE param_hash = ?`, parahHash[:])
	if err != nil {
		if sqlutil.IsErrNoRows(err) {
			return createStagingArea(conn, parahHash)
		}
		return id, err
	}
	return id, nil
}

var _ staging.Storage = (*stagingArea)(nil)

type stagingArea struct {
	conn  *sqlutil.Conn
	rowid int64

	info *marks.Info
	mu   sync.Mutex
}

// newStagingArea returns a stagingArea for the given salt.
// If the staging area does not exist, it will be created.
func newStagingArea(conn *sqlutil.Conn, info *marks.Info) (*stagingArea, error) {
	salt := saltFromBranch(info)
	rowid, err := ensureStagingArea(conn, salt)
	if err != nil {
		return nil, err
	}
	return &stagingArea{conn: conn, rowid: rowid, info: info}, nil
}

func (sa *stagingArea) AreaID() int64 {
	return sa.rowid
}

func (sa *stagingArea) getParamHash() *[32]byte {
	return saltFromBranch(sa.info)
}

func (sa *stagingArea) Put(ctx context.Context, p string, op staging.Operation) error {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	err = sqlutil.Exec(sa.conn, `INSERT INTO staging_ops (area_id, p, data) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`, sa.rowid, p, data)
	return err
}

func (sa *stagingArea) Get(ctx context.Context, p string, dst *staging.Operation) error {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	var data []byte
	if err := sqlutil.Get(sa.conn, &data, `SELECT data FROM staging_ops WHERE area_id = ? AND p = ?`, sa.rowid, p); err != nil {
		if sqlutil.IsErrNoRows(err) {
			return state.ErrNotFound[string]{Key: p}
		}
		return err
	}
	return json.Unmarshal(data, dst)
}

func (sa *stagingArea) List(ctx context.Context, span state.Span[string], buf []string) (int, error) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	var n int
	for p, err := range sqlutil.Select(sa.conn, sqlutil.ScanString, `SELECT p FROM staging_ops WHERE area_id = ? ORDER BY p`, sa.rowid) {
		if err != nil {
			return 0, err
		}
		// TODO: should apply this filtering in the query
		if !span.Contains(p, strings.Compare) {
			continue
		}
		if n >= len(buf) {
			break
		}
		buf[n] = p
		n++
	}
	return n, nil
}

func (sa *stagingArea) Exists(ctx context.Context, p string) (bool, error) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	var exists bool
	err := sqlutil.Get(sa.conn, &exists, `SELECT EXISTS (
		SELECT 1 FROM staging_ops WHERE area_id = ? AND p = ?
	)`, sa.rowid, p)
	return exists, err
}

func (sa *stagingArea) Delete(ctx context.Context, p string) error {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	err := sqlutil.Exec(sa.conn, `DELETE FROM staging_ops WHERE area_id = ? AND p = ?`, sa.rowid, p)
	return err
}

func scan32Bytes(stmt *sqlite.Stmt, dst *[32]byte) error {
	stmt.ColumnBytes(0, dst[:])
	return nil
}
