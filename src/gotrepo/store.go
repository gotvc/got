package gotrepo

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/dbutil"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state/cadata"
)

type CID = blobcache.CID

var _ stores.Writing = &stagingStore{}

// stagingStore stores blobs for a specific staging area.
type stagingStore struct {
	conn    *dbutil.Conn
	areaID  int64
	maxSize int
}

func (ss *stagingStore) Exists(ctx context.Context, cid CID) (bool, error) {
	var exists bool
	if err := dbutil.Get(ss.conn, &exists, `SELECT EXISTS (SELECT 1 FROM staging_blobs WHERE cid = ? AND area_id = ?)`, cid, ss.areaID); err != nil {
		return false, err
	}
	return exists, nil
}

func (ss *stagingStore) Post(ctx context.Context, data []byte) (CID, error) {
	cid := ss.Hash(data)
	if err := dbutil.Exec(ss.conn, `INSERT INTO blobs (cid, data) VALUES (?, ?) ON CONFLICT DO NOTHING`, cid, data); err != nil {
		return CID{}, err
	}
	if err := dbutil.Exec(ss.conn, `INSERT INTO staging_blobs (area_id, cid) VALUES (?, ?) ON CONFLICT DO NOTHING`, ss.areaID, cid); err != nil {
		return CID{}, err
	}
	return cid, nil
}

func (ss *stagingStore) Get(ctx context.Context, cid CID, buf []byte) (int, error) {
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

func cleanupBlobs(conn *dbutil.Conn) error {
	err := dbutil.Exec(conn, `DELETE FROM blobs WHERE NOT EXISTS (SELECT 1 FROM staging_blobs WHERE cid = blobs.cid)`)
	return err
}
