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

var _ staging.Storage = (*stagingArea)(nil)

type stagingArea struct {
	conn *sqlutil.Conn

	info *marks.Info
	mu   sync.Mutex
}

// newStagingArea returns a stagingArea for the given salt.
// If the staging area does not exist, it will be created.
func newStagingArea(conn *sqlutil.Conn, info *marks.Info) *stagingArea {
	return &stagingArea{conn: conn, info: info}
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
	err = sqlutil.Exec(sa.conn, `INSERT INTO staging_ops (p, data) VALUES (?, ?) ON CONFLICT DO NOTHING`, p, data)
	return err
}

func (sa *stagingArea) Get(ctx context.Context, p string, dst *staging.Operation) error {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	var data []byte
	if err := sqlutil.Get(sa.conn, &data, `SELECT data FROM staging_ops WHERE p = ?`, p); err != nil {
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
	for p, err := range sqlutil.Select(sa.conn, sqlutil.ScanString, `SELECT p FROM staging_ops ORDER BY p`) {
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
	)`, p)
	return exists, err
}

func (sa *stagingArea) Delete(ctx context.Context, p string) error {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	err := sqlutil.Exec(sa.conn, `DELETE FROM staging_ops WHERE p = ?`, p)
	return err
}

func scan32Bytes(stmt *sqlite.Stmt, dst *[32]byte) error {
	stmt.ColumnBytes(0, dst[:])
	return nil
}
