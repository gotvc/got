package porting

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotwc/internal/sqlutil"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/tai64"
	"zombiezen.com/go/sqlite"
)

type FileInfo struct {
	ModifiedAt tai64.TAI64N
	Mode       fs.FileMode
}

// DB stores metadata about the state of the directory and
// any data that has been imported from it.
type DB struct {
	conn      *sqlutil.Conn
	paramHash [32]byte
}

func NewDB(conn *sqlutil.Conn, paramHash [32]byte) *DB {
	return &DB{
		conn:      conn,
		paramHash: paramHash,
	}
}

func (db *DB) PutInfo(ctx context.Context, p string, ent FileInfo) error {
	if err := sqlutil.Exec(db.conn, `DELETE FROM fsroots WHERE path = ? AND param_hash = ?`, p, db.paramHash[:]); err != nil {
		return err
	}
	return sqlutil.Exec(db.conn, `INSERT OR REPLACE INTO dirstate (path, mode, modtime) VALUES (?, ?, ?)`, p, uint32(ent.Mode), ent.ModifiedAt.Marshal())
}

func (db *DB) GetInfo(ctx context.Context, p string, dst *FileInfo) (bool, error) {
	return sqlutil.GetOne(db.conn, dst, scanInfo, `SELECT modtime, mode FROM dirstate WHERE path = ?`, p)
}

func (db *DB) PutFSRoot(ctx context.Context, p string, modt tai64.TAI64N, fsroot gotfs.Root) error {
	var info FileInfo
	if ok, err := db.GetInfo(ctx, p, &info); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("cannot add file data before info has been added")
	}
	if info.ModifiedAt != modt {
		return fmt.Errorf("modtime does not match")
	}
	return sqlutil.Exec(db.conn, `UPDATE fsroots
		SET fsroot = ?
		WHERE path = ? AND param_hash = ?
	`, fsroot.Marshal(nil), p, db.paramHash[:])
}

func (db *DB) GetFSRoot(ctx context.Context, p string, dst *gotfs.Root) (bool, error) {
	return sqlutil.GetOne(db.conn, dst, scanFSRoot, `SELECT fsroot FROM fsroots
		WHERE path = ? AND param_hash = ?
	`, p, dst, db.paramHash[:])
}

func (ds *DB) Scan(ctx context.Context, fsys posixfs.FS) error {
	return posixfs.WalkLeaves(ctx, fsys, "", func(p string, dent posixfs.DirEnt) error {
		finfo, err := fsys.Stat(p)
		if err != nil {
			return err
		}
		if err := ds.PutInfo(ctx, p, FileInfo{
			Mode:       dent.Mode,
			ModifiedAt: tai64.FromGoTime(finfo.ModTime()),
		}); err != nil {
			return err
		}
		return nil
	})
}

// scanInfo expects:
// 0: modtime
// 1: mode
func scanInfo(stmt *sqlite.Stmt, dst *FileInfo) error {
	dst.Mode = fs.FileMode(stmt.ColumnInt64(1))
	var modtime [8 + 4]byte
	stmt.ColumnBytes(0, modtime[:])
	return dst.ModifiedAt.UnmarshalBinary(modtime[:])
}

func scanFSRoot(stmt *sqlite.Stmt, dst *gotfs.Root) error {
	var buf [gotfs.RootSize]byte
	stmt.ColumnBytes(0, buf[:])
	return dst.Unmarshal(buf[:])
}
