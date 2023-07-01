package gotrepo

import (
	"context"
	"fmt"
	"os"

	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/brendoncarroll/stdctx/logctx"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/metrics"
	"github.com/gotvc/got/pkg/staging"
	"github.com/gotvc/got/pkg/stores"
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
	porter, err := r.getImporter(ctx, &branch.Info)
	if err != nil {
		return err
	}
	stage := r.getStage()
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
}

// Put replaces a path (file or directory) with whatever is in the working directory
// Adding a file updates the file.
// Adding a directory will delete paths not in the working directory, and add paths in the working directory.
func (r *Repo) Put(ctx context.Context, paths ...string) error {
	branch, err := r.GetBranch(ctx, "")
	if err != nil {
		return err
	}
	porter, err := r.getImporter(ctx, &branch.Info)
	if err != nil {
		return err
	}
	stage := r.stage
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
}

// Rm deletes a path known to version control.
func (r *Repo) Rm(ctx context.Context, paths ...string) error {
	b, err := r.GetBranch(ctx, "")
	if err != nil {
		return err
	}
	vol := b.Volume
	snap, err := branches.GetHead(ctx, vol)
	if err != nil {
		return err
	}
	fsop := r.getFSOp(&b.Info)
	stage := r.getStage()
	for _, target := range paths {
		if snap == nil {
			return fmt.Errorf("path %q not found", target)
		}
		if err := fsop.ForEachLeaf(ctx, vol.FSStore, snap.Root, target, func(p string, _ *gotfs.Info) error {
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

// Clear clears all entries from the staging area
func (r *Repo) Clear(ctx context.Context) error {
	return r.stage.Reset(ctx)
}

type FileOperation struct {
	Delete *staging.DeleteOp

	Create *staging.PutOp
	Modify *staging.PutOp
}

func (r *Repo) ForEachStaging(ctx context.Context, fn func(p string, op FileOperation) error) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	vol := branch.Volume
	fsop := r.getFSOp(&branch.Info)
	snap, err := branches.GetHead(ctx, vol)
	if err != nil {
		return err
	}
	var root gotfs.Root
	if snap != nil {
		root = snap.Root
	} else {
		rootPtr, err := fsop.NewEmpty(ctx, vol.FSStore)
		if err != nil {
			return err
		}
		root = *rootPtr
	}
	return r.stage.ForEach(ctx, func(p string, sop staging.Operation) error {
		var op FileOperation
		switch {
		case sop.Delete != nil:
			op.Delete = sop.Delete
		case sop.Put != nil:
			md, err := fsop.GetInfo(ctx, vol.FSStore, root, p)
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
}

func (r *Repo) Commit(ctx context.Context, snapInfo gotvc.SnapInfo) error {
	if yes, err := r.stage.IsEmpty(ctx); err != nil {
		return err
	} else if yes {
		logctx.Warnf(ctx, "nothing to commit")
		return nil
	}
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	vol := branch.Volume
	snapInfo.Creator = r.GetID().String()
	snapInfo.Authors = append(snapInfo.Authors, r.GetID().String())
	src, err := r.getImportTriple(ctx, &branch.Info)
	if err != nil {
		return err
	}
	dst := vol.StoreTriple()
	// writes go to src, but reads from src should fallback to dst
	src = &branches.StoreTriple{
		Raw: stores.AddWriteLayer(dst.Raw, src.Raw),
		FS:  stores.AddWriteLayer(dst.FS, src.FS),
		VC:  stores.AddWriteLayer(dst.VC, src.VC),
	}
	fsop := r.getFSOp(&branch.Info)
	vcop := r.getVCOp(&branch.Info)
	ctx, cf := metrics.Child(ctx, "applying changes")
	defer cf()
	if err := branches.Apply(ctx, branch.Volume, *src, func(x *Snap) (*Snap, error) {
		var root *Root
		if x != nil {
			root = &x.Root
		}
		nextRoot, err := r.stage.Apply(ctx, fsop, src.FS, src.Raw, root)
		if err != nil {
			return nil, err
		}
		var parents []Snap
		if x != nil {
			parents = []Snap{*x}
		}
		return vcop.NewSnapshot(ctx, src.VC, parents, *nextRoot, snapInfo)
	}); err != nil {
		return err
	}
	return r.getStage().Reset(ctx)
}

// ForEachUntracked lists all the files which are not in either:
//  1. the staging area
//  2. the active branch head
func (r *Repo) ForEachUntracked(ctx context.Context, fn func(p string) error) error {
	_, b, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	snap, err := branches.GetHead(ctx, b.Volume)
	if err != nil {
		return err
	}
	fsop := r.getFSOp(&b.Info)
	return posixfs.WalkLeaves(ctx, r.workingDir, "", func(p string, ent posixfs.DirEnt) error {
		// filter staging
		if op, err := r.stage.Get(ctx, p); err != nil {
			return err
		} else if op != nil {
			return nil
		}
		// filter branch head
		if snap != nil {
			if _, err := fsop.GetInfo(ctx, b.Volume.FSStore, snap.Root, p); err != nil && !os.IsNotExist(err) {
				return err
			} else if err == nil {
				return nil
			}
		}
		return fn(p)
	})
}

func (r *Repo) getStage() *staging.Stage {
	return r.stage
}
