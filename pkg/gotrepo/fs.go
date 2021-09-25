package gotrepo

import (
	"context"
	"io"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
)

func (r *Repo) Ls(ctx context.Context, p string, fn func(gotfs.DirEnt) error) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	snap, err := branches.GetHead(ctx, *branch)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	return r.getFSOp(branch).ReadDir(ctx, branch.Volume.FSStore, snap.Root, p, fn)
}

func (r *Repo) Cat(ctx context.Context, p string, w io.Writer) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	vol := branch.Volume
	snap, err := branches.GetHead(ctx, *branch)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	fr := r.getFSOp(branch).NewReader(ctx, vol.FSStore, vol.RawStore, snap.Root, p)
	_, err = io.Copy(w, fr)
	return err
}

func (r *Repo) Check(ctx context.Context) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	vol := branch.Volume
	snap, err := branches.GetHead(ctx, *branch)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	vcop := r.getVCOp(branch)
	return vcop.Check(ctx, vol.VCStore, *snap, func(root gotfs.Root) error {
		return r.getFSOp(branch).Check(ctx, vol.FSStore, root, func(ref gdat.Ref) error {
			return nil
		})
	})
}
