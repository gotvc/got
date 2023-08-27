package gotrepo

import (
	"context"
	"fmt"
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
	snap, err := branches.GetHead(ctx, branch.Volume)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	return r.getFSOp(&branch.Info).ReadDir(ctx, branch.Volume.FSStore, snap.Root, p, fn)
}

func (r *Repo) Cat(ctx context.Context, p string, w io.Writer) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	vol := branch.Volume
	snap, err := branches.GetHead(ctx, branch.Volume)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	fr, err := r.getFSOp(&branch.Info).NewReader(ctx, vol.FSStore, vol.RawStore, snap.Root, p)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, fr)
	return err
}

func (r *Repo) Stat(ctx context.Context, p string) (*gotfs.Info, error) {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return nil, err
	}
	snap, err := branches.GetHead(ctx, branch.Volume)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, fmt.Errorf("branch is empty")
	}
	fsag := r.getFSOp(&branch.Info)
	return fsag.GetInfo(ctx, branch.Volume.FSStore, snap.Root, p)
}

func (r *Repo) Check(ctx context.Context) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	vol := branch.Volume
	snap, err := branches.GetHead(ctx, vol)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	vcag := r.getVCOp(&branch.Info)
	return vcag.Check(ctx, vol.VCStore, *snap, func(root gotfs.Root) error {
		return r.getFSOp(&branch.Info).Check(ctx, vol.FSStore, root, func(ref gdat.Ref) error {
			return nil
		})
	})
}
