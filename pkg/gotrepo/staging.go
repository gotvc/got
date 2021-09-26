package gotrepo

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/porting"
	"github.com/gotvc/got/pkg/staging"
	"github.com/gotvc/got/pkg/stores"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Add adds paths from the working directory to the staging area.
// Directories are traversed, and only paths are added.
// Adding a directory will update any existing paths and add new ones, it will not remove paths
// from version control
func (r *Repo) Add(ctx context.Context, paths ...string) error {
	branch, err := r.GetBranch(ctx, "")
	if err != nil {
		return err
	}
	storeTriple := r.stagingTriple()
	ms := storeTriple.FS
	ds := storeTriple.Raw
	porter := porting.NewPorter(r.getFSOp(branch), r.workingDir, nil)
	stage := r.getStage()
	for _, target := range paths {
		if err := posixfs.WalkLeaves(ctx, r.workingDir, target, func(p string, _ posixfs.DirEnt) error {
			if err := stage.CheckConflict(ctx, p); err != nil {
				return err
			}
			fileRoot, err := porter.ImportFile(ctx, ms, ds, p)
			if err != nil {
				return err
			}
			return stage.Put(ctx, p, *fileRoot)
		}); err != nil {
			return err
		}
	}
	return nil
}

// Put replaces a path (file or directory) with whatever is in the working directory
// Adding a file updates the file.
// Adding a directory will delete paths not in the working directory, and add paths in the working directory.
func (r *Repo) Put(ctx context.Context, paths ...string) error {
	branch, err := r.GetBranch(ctx, "")
	if err != nil {
		return err
	}
	storeTriple := r.stagingTriple()
	ms := storeTriple.FS
	ds := storeTriple.Raw
	porter := porting.NewPorter(r.getFSOp(branch), r.workingDir, nil)
	stage := r.stage
	for _, p := range paths {
		if err := stage.CheckConflict(ctx, p); err != nil {
			return err
		}
		root, err := porter.ImportPath(ctx, ms, ds, p)
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
}

// Rm deletes a path known to version control.
func (r *Repo) Rm(ctx context.Context, paths ...string) error {
	branch, err := r.GetBranch(ctx, "")
	if err != nil {
		return err
	}
	snap, err := branches.GetHead(ctx, *branch)
	if err != nil {
		return err
	}
	st := r.stagingTriple()
	fsop := r.getFSOp(branch)
	stage := r.getStage()
	for _, target := range paths {
		if snap == nil {
			return errors.Errorf("path %q not found", target)
		}
		if err := fsop.ForEachFile(ctx, st.FS, snap.Root, target, func(p string, _ *gotfs.Metadata) error {
			return stage.Delete(ctx, p)
		}); err != nil {
			return err
		}
	}
	return nil
}

// Discard removes any staged changes for a path
func (r *Repo) Discard(ctx context.Context, paths ...string) error {
	for _, p := range paths {
		if err := r.stage.Discard(ctx, p); err != nil {
			return err
		}
	}
	return nil
}

func (r Repo) Clear(ctx context.Context) error {
	return r.stage.Reset()
}

func (r *Repo) ForEachStaging(ctx context.Context, fn func(p string, fo staging.Operation) error) error {
	return r.stage.ForEach(ctx, fn)
}

func (r *Repo) Commit(ctx context.Context, snapInfo gotvc.SnapInfo) error {
	if yes, err := r.stage.IsEmpty(ctx); err != nil {
		return err
	} else if yes {
		logrus.Warn("nothing to commit")
		return nil
	}
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	snapInfo.Creator = r.GetID()
	src := r.stagingTriple()
	dst := branch.Volume.StoreTriple()
	// writes go to src, but reads from src should fallback to dst
	src = branches.StoreTriple{
		Raw: stores.AddWriteLayer(dst.Raw, src.Raw),
		FS:  stores.AddWriteLayer(dst.FS, src.FS),
		VC:  stores.AddWriteLayer(dst.VC, src.VC),
	}
	fsop := r.getFSOp(branch)
	vcop := r.getVCOp(branch)
	if err := branches.Apply(ctx, *branch, src, func(x *Snap) (*Snap, error) {
		var root *Root
		if x != nil {
			root = &x.Root
		}
		logrus.Println("begin applying staged changes")
		nextRoot, err := r.stage.Apply(ctx, fsop, src.FS, src.Raw, root)
		if err != nil {
			return nil, err
		}
		logrus.Println("done applying staged changes")
		return vcop.NewSnapshot(ctx, src.VC, x, *nextRoot, snapInfo)
	}); err != nil {
		return err
	}
	return r.getStage().Reset()
}

func (r *Repo) stagingStore() cadata.Store {
	return r.storeManager.GetStore(0)
}

func (r *Repo) stagingTriple() branches.StoreTriple {
	return branches.StoreTriple{
		VC:  r.stagingStore(),
		FS:  r.stagingStore(),
		Raw: r.stagingStore(),
	}
}

func (r *Repo) StagingStore() cadata.Store {
	return r.stagingStore()
}

func (r *Repo) getStage() *staging.Stage {
	storage := staging.NewBoltStorage(r.db, bucketStaging)
	return staging.New(storage)
}
