package gotwc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc/internal/sqlutil"
	"github.com/gotvc/got/src/internal/marks"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"
	"go.inet256.org/inet256/src/inet256"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/gotwc/internal/porting"
	"github.com/gotvc/got/src/gotwc/internal/staging"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
)

// DoWithStore runs fn with a store for the desired branch
func (wc *WC) DoWithStore(ctx context.Context, fn func(dst stores.RW) error) error {
	return wc.modifyStaging(ctx, func(sctx stagingCtx) error {
		return fn(sctx.Store)
	})
}

type stagingCtx struct {
	BranchName string
	Branch     marks.Mark

	Store    stores.RW
	Stage    *staging.Tx
	Importer *porting.Importer
	Exporter *porting.Exporter

	FS posixfs.FS

	ActingAs gotorg.IdentityUnit
}

func (wc *WC) modifyStaging(ctx context.Context, fn func(sctx stagingCtx) error) error {
	conn, err := wc.db.Take(ctx)
	if err != nil {
		return err
	}
	defer wc.db.Put(conn)

	// this function is to easily defer the transaction and cleanup.
	if err := func() (retErr error) {
		defer sqlitex.Transaction(conn)(&retErr)

		branchName, err := wc.GetHead()
		if err != nil {
			return err
		}
		idenName, err := wc.GetActAs()
		if err != nil {
			return err
		}
		actAs, err := wc.repo.GetIdentity(ctx, idenName)
		if err != nil {
			return err
		}
		mark, err := wc.repo.GetMark(ctx, gotrepo.FQM{Name: branchName})
		if err != nil {
			return err
		}
		fsys, err := wc.getFilteredFS(ctx)
		if err != nil {
			return err
		}
		paramHash := saltFromBranch(&mark.Info)
		stagetx, err := wc.beginStageTx(ctx, paramHash, true)
		if err != nil {
			return err
		}
		defer stagetx.Abort(ctx)
		stagingStore := stagetx.Store()
		storePair := [2]stores.RW{stagingStore, stagingStore}
		dirState := porting.NewDB(conn, *paramHash)
		imp := porting.NewImporter(mark.GotFS(), dirState, storePair)
		exp := porting.NewExporter(mark.GotFS(), dirState, fsys, wc.moveToTrash)

		if err := fn(stagingCtx{
			BranchName: branchName,
			Branch:     *mark,

			Stage:    stagetx,
			Store:    stagingStore,
			Importer: imp,
			Exporter: exp,

			FS: fsys,

			ActingAs: *actAs,
		}); err != nil {
			return err
		}
		return stagetx.Commit(ctx)
	}(); err != nil {
		return err
	}
	// This has to be done outside of the transaction.
	return sqlutil.WALCheckpoint(conn)
}

func (wc *WC) viewStaging(ctx context.Context, fn func(sctx stagingCtx) error) error {
	return sqlutil.DoTxRO(ctx, wc.db, func(conn *sqlutil.Conn) error {
		branchName, err := wc.GetHead()
		branch, err := wc.repo.GetMark(ctx, gotrepo.FQM{Name: branchName})
		if err != nil {
			return err
		}
		paramHash := saltFromBranch(&branch.Info)
		stagetx, err := wc.beginStageTx(ctx, paramHash, false)
		if err != nil {
			return err
		}
		filtFS, err := wc.getFilteredFS(ctx)
		if err != nil {
			return err
		}
		portdb := porting.NewDB(conn, *paramHash)
		exp := porting.NewExporter(branch.GotFS(), portdb, filtFS, wc.moveToTrash)
		stagingStore, err := wc.repo.BeginStagingTx(ctx, wc.id, false)
		if err != nil {
			return err
		}
		defer stagingStore.Abort(ctx)
		return fn(stagingCtx{
			BranchName: branchName,
			Branch:     *branch,

			Stage:    stagetx,
			Store:    stagingStore,
			Exporter: exp,
		})
	})
}

// Add adds paths from the working directory to the staging area.
// Directories are traversed, and only paths are added.
// Adding a directory will update any existing paths and add new ones, it will not remove paths
// from version control
func (wc *WC) Add(ctx context.Context, paths ...string) error {
	return wc.modifyStaging(ctx, func(sctx stagingCtx) error {
		stage := sctx.Stage
		porter := sctx.Importer
		for _, target := range paths {
			if err := posixfs.WalkLeaves(ctx, sctx.FS, target, func(p string, _ posixfs.DirEnt) error {
				if err := stage.CheckConflict(ctx, p); err != nil {
					return err
				}
				ctx, cf := metrics.Child(ctx, p)
				defer cf()
				fileRoot, err := porter.ImportFile(ctx, sctx.FS, p)
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
func (wc *WC) Put(ctx context.Context, paths ...string) error {
	return wc.modifyStaging(ctx, func(sctx stagingCtx) error {
		stage := sctx.Stage
		porter := sctx.Importer
		for _, p := range paths {
			ctx, cf := metrics.Child(ctx, p)
			defer cf()
			if err := stage.CheckConflict(ctx, p); err != nil {
				return err
			}
			root, err := porter.ImportPath(ctx, sctx.FS, p)
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
func (wc *WC) Rm(ctx context.Context, paths ...string) error {
	return wc.modifyStaging(ctx, func(sctx stagingCtx) error {
		snap, voltx, err := sctx.Branch.GetTarget(ctx)
		if err != nil {
			return err
		}
		defer voltx.Abort(ctx)
		fsag := sctx.Branch.GotFS()
		stage := sctx.Stage
		for _, target := range paths {
			if snap == nil {
				return fmt.Errorf("path %q not found", target)
			}
			if err := fsag.ForEachLeaf(ctx, voltx, snap.Payload.Root, target, func(p string, _ *gotfs.Info) error {
				return stage.Delete(ctx, p)
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Discard removes any staged changes for a path
func (wc *WC) Discard(ctx context.Context, paths ...string) error {
	return wc.modifyStaging(ctx, func(sctx stagingCtx) error {
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
func (wc *WC) Clear(ctx context.Context) error {
	return wc.modifyStaging(ctx, func(sctx stagingCtx) error {
		return sctx.Stage.Clear(ctx)
	})
}

type CommitParams struct {
	// Message is the commit message, it is taken from this value as is.
	Message string
	// Authors is the list of authors of the commit
	// if not-nil, it is taken from this value as is.
	// If nil, then the Author will be the same as the committer.
	Authors []inet256.ID
	// AuthoredAt is the time at which the commit was authored, it is taken from this value as is.
	AuthoredAt tai64.TAI64

	// CommittedAt, if not zero, overrides the commit time
	// If CommittedAt is not zero and not >= that of all the parents,
	// then an error is returned.
	CommittedAt tai64.TAI64
	// Committer if not zero, overrides the ID of the committer
	Committer inet256.ID
}

func (wc *WC) Commit(ctx context.Context, params CommitParams) error {
	return wc.modifyStaging(ctx, func(sctx stagingCtx) error {
		if yes, err := sctx.Stage.IsEmpty(ctx); err != nil {
			return err
		} else if yes {
			logctx.Warnf(ctx, "nothing to commit")
			return nil
		}
		ctx, cf := metrics.Child(ctx, "applying changes")
		defer cf()
		scratch := sctx.Store
		stage := sctx.Stage
		if err := sctx.Branch.Modify(ctx, func(mctx marks.ModifyCtx) (*marks.Snap, error) {
			if params.CommittedAt == 0 {
				params.CommittedAt = tai64.Now().TAI64()
			}
			if params.Committer.IsZero() {
				params.Committer = sctx.ActingAs.ID
			}
			if params.Authors == nil {
				params.Authors = append(params.Authors, params.Committer)
			}
			if params.AuthoredAt == 0 {
				params.AuthoredAt = params.CommittedAt
			}
			var root *gotfs.Root
			if mctx.Root != nil {
				root = &mctx.Root.Payload.Root
			}
			s := stores.AddWriteLayer(mctx.Store, scratch)
			ss := [2]stores.RW{s, s}
			nextRoot, err := stage.Apply(ctx, sctx.Branch.GotFS(), ss, root)
			if err != nil {
				return nil, err
			}
			var parents []marks.Snap
			if mctx.Root != nil {
				parents = []marks.Snap{*mctx.Root}
			}

			infoJSON, err := json.Marshal(struct {
				Authors    []inet256.ID `json:"authors"`
				AuthoredAt tai64.TAI64  `json:"authored_at"`
			}{
				Authors:    params.Authors,
				AuthoredAt: params.AuthoredAt,
			})
			if err != nil {
				return nil, err
			}
			nextSnap, err := sctx.Branch.GotVC().NewSnapshot(ctx, s, gotvc.SnapshotParams[marks.Payload]{
				Parents:   parents,
				Creator:   params.Committer,
				CreatedAt: tai64.Now().TAI64(),
				Payload: marks.Payload{
					Root: *nextRoot,
					Aux:  infoJSON,
				},
			})
			if err := mctx.Sync(ctx, [3]stores.Reading{s, s, s}, *nextSnap); err != nil {
				return nil, err
			}
			return nextSnap, nil
		}); err != nil {
			return err
		}
		if err := stage.Clear(ctx); err != nil {
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

func (wc *WC) ForEachStaging(ctx context.Context, fn func(p string, op FileOperation) error) error {
	return wc.viewStaging(ctx, func(sctx stagingCtx) error {
		stage := sctx.Stage
		snap, voltx, err := sctx.Branch.GetTarget(ctx)
		if err != nil {
			return err
		}
		defer voltx.Abort(ctx)

		// NewEmpty makes a Post which will fail because this is a read-only transaction.
		s := stores.AddWriteLayer(voltx, stores.NewMem())
		var root gotfs.Root
		if snap != nil {
			root = snap.Payload.Root
		} else {
			rootPtr, err := sctx.Branch.GotFS().NewEmpty(ctx, s)
			if err != nil {
				return err
			}
			root = *rootPtr
		}
		return stage.ForEach(ctx, func(ent staging.Entry) error {
			sop := ent.Op
			var op FileOperation
			switch {
			case sop.Delete != nil:
				op.Delete = sop.Delete
			case sop.Put != nil:
				md, err := sctx.Branch.GotFS().GetInfo(ctx, s, root, ent.Path)
				if err != nil && !posixfs.IsErrNotExist(err) {
					return err
				}
				if md == nil {
					op.Create = sop.Put
				} else {
					op.Modify = sop.Put
				}
			}
			return fn(ent.Path, op)
		})
	})
}

// ForEachDirty lists all the files which are not in either:
//  1. the staging area
//  2. the active branch head
func (wc *WC) ForEachDirty(ctx context.Context, fn func(p string, modtime time.Time) error) error {
	return wc.viewStaging(ctx, func(sctx stagingCtx) error {
		snap, voltx, err := sctx.Branch.GetTarget(ctx)
		if err != nil {
			return err
		}
		stage := sctx.Stage
		fsMach := sctx.Branch.GotFS()
		defer voltx.Abort(ctx)
		fsys, err := wc.getFilteredFS(ctx)
		if err != nil {
			return err
		}
		return posixfs.WalkLeaves(ctx, fsys, "", func(p string, ent posixfs.DirEnt) error {
			// filter staging
			if op, err := stage.Get(ctx, p); err != nil {
				return err
			} else if op != nil {
				return nil
			}
			// filter branch head
			if snap != nil {
				if _, err := fsMach.GetInfo(ctx, voltx, snap.Payload.Root, p); err != nil && !os.IsNotExist(err) {
					return err
				} else if err == nil {
					return nil
				}
			}
			finfo, err := fsys.Stat(p)
			if err != nil {
				return err
			}
			return fn(p, finfo.ModTime())
		})
	})
}

// cleanupStagingBlobs removes blobs from staging areas which do not have ops that reference them.
func (wc *WC) cleanupStagingBlobs(ctx context.Context) error {
	tx, err := wc.repo.GCStage(ctx, wc.id)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	kvmach := staging.DefaultGotKV()
	stagetx := staging.New(&kvmach, tx, nil)
	fsmach := gotfs.NewMachine()
	if err := stagetx.ForEach(ctx, func(ent staging.Entry) error {
		fop := ent.Op
		if putOp := fop.Put; putOp != nil {
			// TODO: need to implement set for Tx
			var set cadata.Set
			if err := fsmach.Populate(ctx, tx, *putOp, set, set); err != nil {
				return nil
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func saltFromBranch(b *marks.Info) *[32]byte {
	return (*[32]byte)(&b.Salt)
}
