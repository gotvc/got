package gotrepo

import (
	"context"

	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/porting"
)

// Checkout attempts to actualize the state of the branch in the working directory.
// Checkout is non-destructive, it will only overwrite or delete files which Got is capable of restoring.
// For something a little more dangerous see `Clobber`.
func (r *Repo) Checkout(ctx context.Context, name string, targetPath string) error {
	target, err := r.GetBranch(ctx, name)
	if err != nil {
		return err
	}
	snap, err := branches.GetHead(ctx, *target)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	fsop := r.getFSOp(target)
	// list the files that need to be overwritten
	if err := fsop.ForEachFile(ctx, target.Volume.FSStore, snap.Root, targetPath, func(p string, md *gotfs.Metadata) error {
		// if the file exists and is dirty return an error
		panic("")
	}); err != nil {
		return err
	}
	// list the files that need to be deleted
	if err := posixfs.WalkLeaves(ctx, r.workingDir, targetPath, func(p string, ent posixfs.DirEnt) error {
		// if the file should not exist and is dirty, return an error
		panic("")
	}); err != nil {
		return err
	}
	// overwrite files
	if err := fsop.ForEachFile(ctx, target.Volume.FSStore, snap.Root, targetPath, func(p string, md *gotfs.Metadata) error {
		return r.exportFile(ctx, *target, fsop, snap.Root, p)
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

func (r *Repo) exportFile(ctx context.Context, b branches.Branch, fsop *gotfs.Operator, root gotfs.Root, p string) error {
	porter := porting.NewPorter(fsop, r.workingDir, r.getPorterCache(b))
	return porter.ExportFile(ctx, b.Volume.FSStore, b.Volume.RawStore, root, p)
}

func (r *Repo) deleteWorkingFile(ctx context.Context, p string) error {
	if p == "" {
		panic("cannot delete working dir root")
	}
	return r.workingDir.Remove(p)
}
