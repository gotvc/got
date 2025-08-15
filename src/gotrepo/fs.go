package gotrepo

import (
	"context"
	"fmt"
	"io"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/stores"
)

func (r *Repo) Ls(ctx context.Context, p string, fn func(gotfs.DirEnt) error) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	snap, tx, err := branches.GetHead(ctx, branch.Volume)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if snap == nil {
		return nil
	}
	return branches.NewGotFS(&branch.Info).ReadDir(ctx, tx, snap.Root, p, fn)
}

func (r *Repo) Cat(ctx context.Context, p string, w io.Writer) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	snap, tx, err := branches.GetHead(ctx, branch.Volume)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if snap == nil {
		return nil
	}
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	fr, err := branches.NewGotFS(&branch.Info).NewReader(ctx, [2]stores.Reading{tx, tx}, snap.Root, p)
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
	snap, tx, err := branches.GetHead(ctx, branch.Volume)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, fmt.Errorf("branch is empty")
	}
	fsag := branches.NewGotFS(&branch.Info)
	return fsag.GetInfo(ctx, tx, snap.Root, p)
}

func (r *Repo) Check(ctx context.Context) error {
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	snap, tx, err := branches.GetHead(ctx, branch.Volume)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	vcag := branches.NewGotVC(&branch.Info)
	return vcag.Check(ctx, tx, *snap, func(root gotfs.Root) error {
		return branches.NewGotFS(&branch.Info).Check(ctx, tx, root, func(ref gdat.Ref) error {
			return nil
		})
	})
}
