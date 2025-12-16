package gotrepo

import (
	"context"
	"io"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/stores"
)

func (r *Repo) Ls(ctx context.Context, branchName string, p string, fn func(gotfs.DirEnt) error) error {
	branch, err := r.GetBranch(ctx, branchName)
	if err != nil {
		return err
	}
	return branch.ViewFS(ctx, func(mach *gotfs.Machine, stores stores.Reading, root gotfs.Root) error {
		return mach.ReadDir(ctx, stores, root, p, fn)
	})
}

func (r *Repo) Cat(ctx context.Context, branchName, p string, w io.Writer) error {
	branch, err := r.GetBranch(ctx, branchName)
	if err != nil {
		return err
	}
	return branch.ViewFS(ctx, func(mach *gotfs.Machine, s stores.Reading, root gotfs.Root) error {
		fr, err := mach.NewReader(ctx, [2]stores.Reading{s, s}, root, p)
		if err != nil {
			return err
		}
		_, err = io.Copy(w, fr)
		return err
	})
}

func (r *Repo) Stat(ctx context.Context, branchName, p string) (*gotfs.Info, error) {
	branch, err := r.GetBranch(ctx, branchName)
	if err != nil {
		return nil, err
	}
	var info *gotfs.Info
	if err := branch.ViewFS(ctx, func(mach *gotfs.Machine, s stores.Reading, root gotfs.Root) error {
		var err error
		info, err = mach.GetInfo(ctx, s, root, p)
		return err
	}); err != nil {
		return nil, err
	}
	return info, nil
}

func (r *Repo) Check(ctx context.Context, branchName string) error {
	branch, err := r.GetBranch(ctx, branchName)
	if err != nil {
		return err
	}
	snap, tx, err := branch.GetTarget(ctx)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if snap == nil {
		return nil
	}
	vcag := branch.GotVC()
	return vcag.Check(ctx, tx, *snap, func(payload gotvc.Payload) error {
		return branch.GotFS().Check(ctx, tx, payload.Root, func(ref gdat.Ref) error {
			return nil
		})
	})
}
