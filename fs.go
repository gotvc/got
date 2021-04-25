package got

import (
	"context"
	"io"

	"github.com/brendoncarroll/got/pkg/gotfs"
)

func (r *Repo) Ls(ctx context.Context, p string, fn func(gotfs.DirEnt) error) error {
	_, vol, err := r.GetActiveVolume(ctx)
	if err != nil {
		return err
	}
	snap, err := getSnapshot(ctx, vol.Cell)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	return r.getFSOp().ReadDir(ctx, vol.FSStore, snap.Root, p, fn)
}

func (r *Repo) Cat(ctx context.Context, p string, w io.Writer) error {
	_, vol, err := r.GetActiveVolume(ctx)
	if err != nil {
		return err
	}
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
