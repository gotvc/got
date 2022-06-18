package gotrepo

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/porting"
	"github.com/pkg/errors"
)

// Checkout attempts to actualize the state of the branch in the working directory.
// Checkout is non-destructive, it will only overwrite or delete files which Got is capable of restoring.
// For something a little more dangerous see `Clobber`.
func (r *Repo) Checkout(ctx context.Context, name string, paths []string) error {
	if err := r.SetActiveBranch(ctx, ""); err != nil {
		return err
	}
	b, err := r.GetBranch(ctx, name)
	if err != nil {
		return err
	}
	snap, err := branches.GetHead(ctx, *b)
	if err != nil {
		return err
	}
	if snap == nil {
		return errors.New("empty branch")
	}
	fsop := r.getFSOp(b)
	porter := porting.NewPorter(fsop, r.workingDir, r.getPorterCache(*b))
	if paths != nil {
		for _, p := range paths {
			if err := r.checkoutPath(ctx, porter, fsop, b.Volume.FSStore, b.Volume.RawStore, snap.Root, p); err != nil {
				return err
			}
		}
	} else {
		return r.checkoutPath(ctx, porter, fsop, b.Volume.FSStore, b.Volume.RawStore, snap.Root, "")
	}
	return nil
}

func (r *Repo) checkoutPath(ctx context.Context, porter porting.Porter, fsop *gotfs.Operator, ms, ds cadata.Store, root gotfs.Root, targetPath string) error {
	// list the files that need to be overwritten
	if err := fsop.ForEachFile(ctx, ms, root, targetPath, func(p string, md *gotfs.Info) error {
		// if the file exists and is dirty return an error
		yes, err := porter.IsKnown(ctx, p)
		if err != nil {
			return err
		}
		if !yes {
			return fmt.Errorf("file %q is not known to got. checkout would overwrite", p)
		}
		return nil
	}); err != nil {
		return err
	}
	// list the files that need to be deleted
	if err := posixfs.WalkLeaves(ctx, r.workingDir, targetPath, func(p string, ent posixfs.DirEnt) error {
		yes, err := porter.IsKnown(ctx, p)
		if err != nil {
			return err
		}
		if !yes {
			return fmt.Errorf("file %q is not known to got. checkout would overwrite", p)
		}
		return nil
	}); err != nil {
		return err
	}
	return r.clobberPath(ctx, porter, fsop, ms, ds, root, targetPath)
}

// Clobber overwrites the path at p with whatever is in the head of the branch, and does not perform any safety checks first.
func (r *Repo) Clobber(ctx context.Context, branchName string, p string) error {
	b, err := r.GetBranch(ctx, branchName)
	if err != nil {
		return err
	}
	snap, err := branches.GetHead(ctx, *b)
	if err != nil {
		return err
	}
	if snap == nil {
		return errors.New("empty branch")
	}
	fsop := r.getFSOp(b)
	porter := porting.NewPorter(fsop, r.workingDir, r.getPorterCache(*b))
	return r.clobberPath(ctx, porter, fsop, b.Volume.FSStore, b.Volume.RawStore, snap.Root, p)
}

// clobber overwrites the file
func (r *Repo) clobberPath(ctx context.Context, porter porting.Porter, fsop *gotfs.Operator, ms, ds cadata.Store, root gotfs.Root, targetPath string) error {
	// overwrite files
	if err := fsop.ForEachFile(ctx, ms, root, targetPath, func(p string, md *gotfs.Info) error {
		return porter.ExportFile(ctx, ms, ds, root, p)
	}); err != nil {
		return err
	}
	// delete files
	if err := posixfs.WalkLeaves(ctx, r.workingDir, targetPath, func(p string, ent posixfs.DirEnt) error {
		// if the file should not exist and is dirty, return an error
		return r.deleteWorkingFile(ctx, p)
	}); err != nil {
		return err
	}
	return nil
}

func (r *Repo) deleteWorkingFile(ctx context.Context, p string) error {
	if p == "" {
		return errors.New("cannot delete working dir root")
	}
	return r.workingDir.Remove(p)
}
