package gotrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/gotvc/got/src/internal/dbutil"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/porting"
	"github.com/gotvc/got/src/internal/staging"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/gotvc/got/src/internal/units"
)

type stagingCtx struct {
	BranchName string
	Branch     Branch

	FSMach   *gotfs.Machine
	VCMach   *gotvc.Machine
	Store    stores.RW
	Stage    *staging.Stage
	Importer *porting.Importer
	Exporter *porting.Exporter

	ActingAs gotns.IdentityLeaf
}

func (r *Repo) modifyStaging(ctx context.Context, fn func(sctx stagingCtx) error) (retErr error) {
	conn, err := r.db.Take(ctx)
	if err != nil {
		return err
	}
	defer r.db.Put(conn)
	defer sqlitex.Transaction(conn)(&retErr)

	branchName, err := getActiveBranch(conn)
	if err != nil {
		return err
	}
	actAs, err := r.ActiveIdentity(ctx)
	if err != nil {
		return err
	}
	branch, err := r.getBranch(ctx, branchName)
	if err != nil {
		return err
	}
	sa, err := newStagingArea(conn, &branch.Info)
	if err != nil {
		return err
	}
	dirState := newDirState(conn, gdat.Hash(sa.getSalt()[:]))
	imp := porting.NewImporter(branches.NewGotFS(&branch.Info), dirState, [2]stores.RW{sa.getStore(), sa.getStore()})
	exp := porting.NewExporter(branches.NewGotFS(&branch.Info), dirState, r.workingDir)
	fsMach := branches.NewGotFS(&branch.Info)
	const maxSize = 1 << 21
	if err := fn(stagingCtx{
		BranchName: branchName,
		Branch:     *branch,
		FSMach:     fsMach,
		VCMach:     branches.NewGotVC(&branch.Info),

		Stage:    staging.New(sa),
		Store:    &stagingStore{conn: conn, areaID: sa.AreaID(), maxSize: maxSize},
		Importer: imp,
		Exporter: exp,
		ActingAs: actAs,
	}); err != nil {
		return err
	}
	return nil
}

func (r *Repo) viewStaging(ctx context.Context, fn func(sctx stagingCtx) error) error {
	return dbutil.DoTxRO(ctx, r.db, func(conn *dbutil.Conn) error {
		branchName, branch, err := r.GetActiveBranch(ctx)
		if err != nil {
			return err
		}
		sa, err := newStagingArea(conn, &branch.Info)
		if err != nil {
			return err
		}
		dirState := newDirState(conn, gdat.Hash(sa.getSalt()[:]))
		exp := porting.NewExporter(branches.NewGotFS(&branch.Info), dirState, r.workingDir)
		const maxSize = 1 << 21
		return fn(stagingCtx{
			BranchName: branchName,
			Branch:     *branch,
			FSMach:     branches.NewGotFS(&branch.Info),
			VCMach:     branches.NewGotVC(&branch.Info),

			Stage:    staging.New(sa),
			Store:    &stagingStore{conn: conn, areaID: sa.AreaID(), maxSize: maxSize},
			Exporter: exp,
		})
	})
}

// Add adds paths from the working directory to the staging area.
// Directories are traversed, and only paths are added.
// Adding a directory will update any existing paths and add new ones, it will not remove paths
// from version control
func (r *Repo) Add(ctx context.Context, paths ...string) error {
	return r.modifyStaging(ctx, func(sctx stagingCtx) error {
		stage := sctx.Stage
		porter := sctx.Importer
		for _, target := range paths {
			if err := posixfs.WalkLeaves(ctx, r.workingDir, target, func(p string, _ posixfs.DirEnt) error {
				if err := stage.CheckConflict(ctx, p); err != nil {
					return err
				}
				ctx, cf := metrics.Child(ctx, p)
				defer cf()
				fileRoot, err := porter.ImportFile(ctx, r.workingDir, p)
				if err != nil {
					return err
				}
				return stage.Put(ctx, p, *fileRoot)
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Put replaces a path (file or directory) with whatever is in the working directory
// Adding a file updates the file.
// Adding a directory will delete paths not in the working directory, and add paths in the working directory.
func (r *Repo) Put(ctx context.Context, paths ...string) error {
	return r.modifyStaging(ctx, func(sctx stagingCtx) error {
		stage := sctx.Stage
		porter := sctx.Importer
		for _, p := range paths {
			ctx, cf := metrics.Child(ctx, p)
			defer cf()
			if err := stage.CheckConflict(ctx, p); err != nil {
				return err
			}
			root, err := porter.ImportPath(ctx, r.workingDir, p)
			if err != nil && !posixfs.IsErrNotExist(err) {
				return err
			}
			if posixfs.IsErrNotExist(err) {
				if err := stage.Delete(ctx, p); err != nil {
					return err
				}
			} else {
				if err := stage.Put(ctx, p, *root); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// Rm deletes a path known to version control.
func (r *Repo) Rm(ctx context.Context, paths ...string) error {
	return r.modifyStaging(ctx, func(sctx stagingCtx) error {
		vol := sctx.Branch.Volume
		snap, voltx, err := branches.GetHead(ctx, vol)
		if err != nil {
			return err
		}
		defer voltx.Abort(ctx)
		fsag := sctx.FSMach
		stage := sctx.Stage
		for _, target := range paths {
			if snap == nil {
				return fmt.Errorf("path %q not found", target)
			}
			if err := fsag.ForEachLeaf(ctx, voltx, snap.Root, target, func(p string, _ *gotfs.Info) error {
				return stage.Delete(ctx, p)
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Discard removes any staged changes for a path
func (r *Repo) Discard(ctx context.Context, paths ...string) error {
	return r.modifyStaging(ctx, func(sctx stagingCtx) error {
		stage := sctx.Stage
		for _, p := range paths {
			if err := stage.Discard(ctx, p); err != nil {
				return err
			}
		}
		return nil
	})
}

// Clear clears all entries from the staging area
func (r *Repo) Clear(ctx context.Context) error {
	return r.modifyStaging(ctx, func(sctx stagingCtx) error {
		if err := sctx.Stage.Reset(ctx); err != nil {
			return err
		}
		return nil
	})
}

func (r *Repo) Commit(ctx context.Context, snapInfo gotvc.SnapInfo) error {
	return r.modifyStaging(ctx, func(sctx stagingCtx) error {
		if yes, err := sctx.Stage.IsEmpty(ctx); err != nil {
			return err
		} else if yes {
			logctx.Warnf(ctx, "nothing to commit")
			return nil
		}
		snapInfo.Creator = sctx.ActingAs.ID
		snapInfo.Authors = append(snapInfo.Authors, sctx.ActingAs.ID)
		ctx, cf := metrics.Child(ctx, "applying changes")
		defer cf()
		src := sctx.Store
		branch := sctx.Branch
		stage := sctx.Stage
		fsMach := sctx.FSMach
		vcMach := sctx.VCMach
		if err := branches.Apply(ctx, branch.Volume, src, func(dst stores.RW, x *Snap) (*Snap, error) {
			var root *Root
			if x != nil {
				root = &x.Root
			}
			nextRoot, err := stage.Apply(ctx, fsMach, [2]stores.RW{src, src}, root)
			if err != nil {
				return nil, err
			}
			var parents []Snap
			if x != nil {
				parents = []Snap{*x}
			}
			return vcMach.NewSnapshot(ctx, src, parents, *nextRoot, snapInfo)
		}); err != nil {
			return err
		}
		if err := stage.Reset(ctx); err != nil {
			return err
		}
		return nil
	})
}

type FileOperation struct {
	Delete *staging.DeleteOp

	Create *staging.PutOp
	Modify *staging.PutOp
}

func (r *Repo) ForEachStaging(ctx context.Context, fn func(p string, op FileOperation) error) error {
	return r.viewStaging(ctx, func(sctx stagingCtx) error {
		stage := sctx.Stage
		fsag := sctx.FSMach
		vol := sctx.Branch.Volume
		snap, voltx, err := branches.GetHead(ctx, vol)
		if err != nil {
			return err
		}
		defer voltx.Abort(ctx)
		var root gotfs.Root
		if snap != nil {
			root = snap.Root
		} else {
			rootPtr, err := fsag.NewEmpty(ctx, voltx)
			if err != nil {
				return err
			}
			root = *rootPtr
		}
		return stage.ForEach(ctx, func(p string, sop staging.Operation) error {
			var op FileOperation
			switch {
			case sop.Delete != nil:
				op.Delete = sop.Delete
			case sop.Put != nil:
				md, err := fsag.GetInfo(ctx, voltx, root, p)
				if err != nil && !posixfs.IsErrNotExist(err) {
					return err
				}
				if md == nil {
					op.Create = sop.Put
				} else {
					op.Modify = sop.Put
				}
			}
			return fn(p, op)
		})
	})
}

// ForEachUntracked lists all the files which are not in either:
//  1. the staging area
//  2. the active branch head
func (r *Repo) ForEachUntracked(ctx context.Context, fn func(p string) error) error {
	return r.viewStaging(ctx, func(sctx stagingCtx) error {
		snap, voltx, err := branches.GetHead(ctx, sctx.Branch.Volume)
		if err != nil {
			return err
		}
		stage := sctx.Stage
		fsMach := sctx.FSMach
		defer voltx.Abort(ctx)
		return posixfs.WalkLeaves(ctx, r.workingDir, "", func(p string, ent posixfs.DirEnt) error {
			// filter staging
			if op, err := stage.Get(ctx, p); err != nil {
				return err
			} else if op != nil {
				return nil
			}
			// filter branch head
			if snap != nil {
				if _, err := fsMach.GetInfo(ctx, voltx, snap.Root, p); err != nil && !os.IsNotExist(err) {
					return err
				} else if err == nil {
					return nil
				}
			}
			return fn(p)
		})
	})

}

// createStagingArea creates a new staging area and returns its id.
func createStagingArea(conn *dbutil.Conn, salt *[32]byte) (int64, error) {
	var rowid int64
	err := dbutil.Get(conn, &rowid, `INSERT INTO staging_areas (salt) VALUES (?) RETURNING rowid`, salt[:])
	if err != nil {
		return 0, err
	}
	return rowid, err
}

// ensureStagingArea finds the staging area with the given salt, or creates a new one if it doesn't exist.
func ensureStagingArea(conn *dbutil.Conn, salt *[32]byte) (int64, error) {
	var id int64
	err := dbutil.Get(conn, &id, `SELECT rowid FROM staging_areas WHERE salt = ?`, salt[:])
	if err != nil {
		if err.Error() == "no rows found" {
			return createStagingArea(conn, salt)
		}
		return id, err
	}
	return id, nil
}

var _ staging.Storage = (*stagingArea)(nil)

type stagingArea struct {
	conn  *dbutil.Conn
	rowid int64

	info *branches.Info
	mu   sync.Mutex
}

// newStagingArea returns a stagingArea for the given salt.
// If the staging area does not exist, it will be created.
func newStagingArea(conn *dbutil.Conn, info *branches.Info) (*stagingArea, error) {
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

func (sa *stagingArea) getSalt() *[32]byte {
	return saltFromBranch(sa.info)
}

func (sa *stagingArea) getStore() stores.RW {
	return &stagingStore{conn: sa.conn, areaID: sa.rowid, maxSize: 1 << 21}
}

func (sa *stagingArea) Put(ctx context.Context, p string, op staging.Operation) error {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	err = dbutil.Exec(sa.conn, `INSERT INTO staging_ops (area_id, p, data) VALUES (?, ?, ?)`, sa.rowid, p, data)
	return err
}

func (sa *stagingArea) Get(ctx context.Context, p string, dst *staging.Operation) error {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	var data []byte
	if err := dbutil.Get(sa.conn, &data, `SELECT data FROM staging_ops WHERE area_id = ? AND p = ?`, sa.rowid, p); err != nil {
		if err.Error() == "no rows found" {
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
	for p, err := range dbutil.Select(sa.conn, dbutil.ScanString, `SELECT p FROM staging_ops WHERE area_id = ? ORDER BY p`, sa.rowid) {
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
	err := dbutil.Get(sa.conn, &exists, `SELECT EXISTS (
		SELECT 1 FROM staging_ops WHERE area_id = ? AND p = ?
	)`, sa.rowid, p)
	return exists, err
}

func (sa *stagingArea) Delete(ctx context.Context, p string) error {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	err := dbutil.Exec(sa.conn, `DELETE FROM staging_ops WHERE area_id = ? AND p = ?`, sa.rowid, p)
	return err
}

// cleanupStagingBlobs removes blobs from staging areas which do not have ops that reference them.
func cleanupStagingBlobs(ctx context.Context, conn *dbutil.Conn) error {
	// get all of the ids for the empty staging areas
	var areaIDs []int64
	for areaID, err := range dbutil.Select(conn, dbutil.ScanInt64, `SELECT rowid FROM staging_areas WHERE NOT EXISTS (SELECT 1 FROM staging_ops WHERE area_id = rowid)`) {
		if err != nil {
			return err
		}
		areaIDs = append(areaIDs, areaID)
	}
	metrics.SetDenom(ctx, "staging_areas", len(areaIDs), units.None)
	// if the staging area has no ops, then it has no blobs either.
	for _, areaID := range areaIDs {
		if err := dbutil.Exec(conn, `DELETE FROM staging_blobs WHERE area_id = ?`, areaID); err != nil {
			return err
		}
		metrics.AddInt(ctx, "staging_areas", 1, units.None)
	}
	return nil
}

func saltFromBranch(b *branches.Info) *[32]byte {
	if b.Salt == nil {
		return new([32]byte)
	}
	return (*[32]byte)(b.Salt)
}
