package gotwc

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"

	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc/internal/sqlutil"
	"github.com/gotvc/got/src/internal/marks"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/posixfs"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"
	"go.inet256.org/inet256/src/inet256"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/gotvc/got/src/gotfs"
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
	Stage    *staging.Tx
	GotFS    *gotfs.Machine
	Store    stores.RW
	FS       posixfs.FS
	DB       *porting.DB
	Importer *porting.Importer
	Exporter *porting.Exporter
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
		mark, err := wc.repo.GetMark(ctx, gotrepo.FQM{Name: branchName})
		if err != nil {
			return err
		}
		fsys, filter, err := wc.getFilteredFS(ctx)
		if err != nil {
			return err
		}
		paramHash := mark.Config().Hash()
		stagetx, err := wc.beginStageTx(ctx, &paramHash, true)
		if err != nil {
			return err
		}
		defer stagetx.Abort(ctx)
		stagingStore := stagetx.Store()
		storePair := [2]stores.RW{stagingStore, stagingStore}
		dirState := porting.NewDB(conn, paramHash)
		imp := porting.NewImporter(mark.GotFS(), dirState, storePair)
		exp := porting.NewExporter(mark.GotFS(), dirState, fsys, filter)

		if err := fn(stagingCtx{
			Stage:    stagetx,
			Store:    stagingStore,
			FS:       fsys,
			DB:       porting.NewDB(conn, paramHash),
			Importer: imp,
			Exporter: exp,
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
		paramHash := branch.Info.Config.Hash()
		stagetx, err := wc.beginStageTx(ctx, &paramHash, false)
		if err != nil {
			return err
		}
		filtFS, filter, err := wc.getFilteredFS(ctx)
		if err != nil {
			return err
		}
		portdb := porting.NewDB(conn, paramHash)
		exp := porting.NewExporter(branch.GotFS(), portdb, filtFS, filter)
		stagingStore, err := wc.repo.BeginStagingTx(ctx, wc.id, false)
		if err != nil {
			return err
		}
		defer stagingStore.Abort(ctx)
		return fn(stagingCtx{
			GotFS:    branch.GotFS(),
			Stage:    stagetx,
			Store:    stagingStore,
			Exporter: exp,
			DB:       portdb,
		})
	})
}

func (wc *WC) viewMark(ctx context.Context) (*marks.Snap, marks.Tx, error) {
	name, err := wc.GetHead()
	if err != nil {
		return nil, nil, err
	}
	mark, err := wc.repo.GetMark(ctx, gotrepo.FQM{Name: name})
	if err != nil {
		return nil, nil, err
	}
	return mark.GetTarget(ctx)
}

func (wc *WC) modifyMark(ctx context.Context, fn func(marks.ModifyCtx) (*marks.Snap, error)) error {
	name, err := wc.GetHead()
	if err != nil {
		return err
	}
	mark, err := wc.repo.GetMark(ctx, gotrepo.FQM{Name: name})
	if err != nil {
		return err
	}
	return mark.Modify(ctx, fn)
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
		snap, voltx, err := wc.viewMark(ctx)
		if err != nil {
			return err
		}
		defer voltx.Abort(ctx)
		fsag := sctx.GotFS
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

		if params.CommittedAt == 0 {
			params.CommittedAt = tai64.Now().TAI64()
		}
		if params.Committer.IsZero() {
			actAs, err := wc.GetActAs()
			if err != nil {
				return err
			}
			idu, err := wc.repo.GetIdentity(ctx, actAs)
			if err != nil {
				return err
			}
			params.Committer = idu.ID
		}
		if params.Authors == nil {
			params.Authors = append(params.Authors, params.Committer)
		}
		if params.AuthoredAt == 0 {
			params.AuthoredAt = params.CommittedAt
		}

		if err := wc.modifyMark(ctx, func(mctx marks.ModifyCtx) (*marks.Snap, error) {
			var root *gotfs.Root
			if mctx.Root != nil {
				root = &mctx.Root.Payload.Root
			}
			s := stores.AddWriteLayer(mctx.Store, scratch)
			ss := [2]stores.RW{s, s}
			nextRoot, err := stage.Apply(ctx, mctx.FS, ss, root)
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
			nextSnap, err := mctx.VC.NewSnapshot(ctx, s, gotvc.SnapshotParams[marks.Payload]{
				Parents:   parents,
				Creator:   params.Committer,
				CreatedAt: params.CommittedAt,
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
		snap, voltx, err := wc.viewMark(ctx)
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
			rootPtr, err := sctx.GotFS.NewEmpty(ctx, s)
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
				md, err := sctx.GotFS.GetInfo(ctx, s, root, ent.Path)
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

// DirtyFile is a file that has changed in the working copy.
type DirtyFile struct {
	Path string

	// If true than the file exists in the working copy.
	Exists     bool
	Mode       fs.FileMode
	ModifiedAt tai64.TAI64N
}

type FileInfo = porting.FileInfo

// ForEachDirty lists all the files which are not in either:
//  1. the staging area
//  2. the active branch head
func (wc *WC) ForEachDirty(ctx context.Context, fn func(fi DirtyFile) error) error {
	return wc.viewStaging(ctx, func(sctx stagingCtx) error {
		_, voltx, err := wc.viewMark(ctx)
		if err != nil {
			return err
		}
		defer voltx.Abort(ctx)
		stage := sctx.Stage
		fsys, _, err := wc.getFilteredFS(ctx)
		if err != nil {
			return err
		}
		// fsMach := sctx.GotFS
		// spans, err := wc.ListSpans(ctx)
		// if err != nil {
		// 	return err
		// }
		// _ = newGotFSInfoIter(fsMach, voltx, snap.Payload.Root, spans)
		uk := wc.newUnknownIterator(sctx.DB, fsys)
		return streams.ForEach(ctx, uk, func(ukp unknownFile) error {
			p := ukp.Path()
			// filter staging
			var op staging.Operation
			if found, err := stage.Get(ctx, p, &op); err != nil {
				return err
			} else if found {
				if op.Delete != nil && !ukp.Current.Ok {
					// File is gone, and staging deleted it, skip.
					return nil
				}
				// If it is a Put operation, then it is definitely different,
				// otherwise it would be in the database, and would have been filtered by the matching join.
			}
			return fn(DirtyFile{
				Path:       p,
				Exists:     ukp.Current.Ok,
				Mode:       ukp.Current.X.Mode,
				ModifiedAt: ukp.Current.X.ModifiedAt,
			})
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
