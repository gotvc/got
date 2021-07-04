package got

import (
	"context"
	"io"

	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotvc"
)

func (r *Repo) Ls(ctx context.Context, p string, fn func(gotfs.DirEnt) error) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	snap, err := getSnapshot(ctx, branch.Volume.Cell)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	return r.getFSOp().ReadDir(ctx, branch.Volume.FSStore, snap.Root, p, fn)
}

func (r *Repo) Cat(ctx context.Context, p string, w io.Writer) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	vol := branch.Volume
	snap, err := getSnapshot(ctx, vol.Cell)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	fr := r.getFSOp().NewReader(ctx, vol.FSStore, vol.RawStore, snap.Root, p)
	_, err = io.Copy(w, fr)
	return err
}

func (r *Repo) Check(ctx context.Context) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	vol := branch.Volume
	snap, err := getSnapshot(ctx, vol.Cell)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	return gotvc.Check(ctx, vol.VCStore, *snap, func(root gotfs.Root) error {
		return r.getFSOp().Check(ctx, vol.FSStore, root, func(ref gdat.Ref) error {
			return nil
		})
	})
}
