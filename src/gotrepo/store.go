package gotrepo

import (
	"context"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/dbutil"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state/cadata"
	"zombiezen.com/go/sqlite"
)

type CID = blobcache.CID

var _ stores.Writing = &stagingStore{}

// stagingStore stores blobs for a specific staging area.
type stagingStore struct {
	conn    *dbutil.Conn
	areaID  int64
	maxSize int

	postBlobStmt *sqlite.Stmt
	addBlobStmt  *sqlite.Stmt
	mu           sync.Mutex
}

func newStagingStore(conn *dbutil.Conn, areaID int64) *stagingStore {
	const maxSize = 1 << 21
	return &stagingStore{
		conn:    conn,
		areaID:  areaID,
		maxSize: maxSize,

		postBlobStmt: conn.Prep(`INSERT INTO blobs (cid, data) VALUES (?, ?) ON CONFLICT DO NOTHING`),
		addBlobStmt:  conn.Prep(`INSERT INTO staging_blobs (area_id, cid) VALUES (?, ?) ON CONFLICT DO NOTHING`),
	}
}

func (ss *stagingStore) Exists(ctx context.Context, cid CID) (bool, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	var exists bool
	if err := dbutil.Get(ss.conn, &exists, `SELECT EXISTS (SELECT 1 FROM staging_blobs WHERE cid = ? AND area_id = ?)`, cid, ss.areaID); err != nil {
		return false, err
	}
	return exists, nil
}

func (ss *stagingStore) Post(ctx context.Context, data []byte) (CID, error) {
	cid := ss.Hash(data)
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if err := ss.postBlobStmt.Reset(); err != nil {
		return CID{}, err
	}
	ss.postBlobStmt.BindBytes(1, cid[:])
	if data == nil {
		data = []byte{}
	}
	ss.postBlobStmt.BindBytes(2, data)
	if ok, err := ss.postBlobStmt.Step(); err != nil {
		return CID{}, err
	} else if ok {
		return cid, fmt.Errorf("not expecting rows")
	}

	if err := ss.addBlobStmt.Reset(); err != nil {
		return CID{}, err
	}
	ss.addBlobStmt.BindInt64(1, ss.areaID)
	ss.addBlobStmt.BindBytes(2, cid[:])
	if ok, err := ss.addBlobStmt.Step(); err != nil {
		return CID{}, err
	} else if ok {
		return cid, fmt.Errorf("not expecting rows")
	}

	return cid, nil
}

func (ss *stagingStore) Get(ctx context.Context, cid CID, buf []byte) (int, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	var data []byte
	err := dbutil.Get(ss.conn, &data, `
		SELECT b.data
		FROM staging_blobs sb
		JOIN blobs b ON sb.cid = b.cid
		WHERE sb.area_id = ? AND sb.cid = ?
	`, ss.areaID, cid)
	if err != nil {
		if err.Error() == "no rows found" {
			return 0, cadata.ErrNotFound{Key: cid}
		}
		return 0, err
	}
	return copy(buf, data), nil
}

func (ss *stagingStore) Hash(data []byte) CID {
	return gdat.Hash(data)
}

func (ss *stagingStore) MaxSize() int {
	return ss.maxSize
}

func (ss *stagingStore) Close() error {
	for _, stmt := range []*sqlite.Stmt{ss.postBlobStmt, ss.addBlobStmt} {
		if stmt != nil {
			if err := stmt.Finalize(); err != nil {
				return err
			}
		}
	}
	return nil
}

func cleanupBlobs(conn *dbutil.Conn) error {
	err := dbutil.Exec(conn, `DELETE FROM blobs WHERE NOT EXISTS (SELECT 1 FROM staging_blobs WHERE cid = blobs.cid)`)
	return err
}
