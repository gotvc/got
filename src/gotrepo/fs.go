package gotrepo

import (
	"context"
	"io"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/marks"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/stdctx/logctx"
)

func (r *Repo) Ls(ctx context.Context, mark FQM, p string, fn func(gotfs.DirEnt) error) error {
	branch, err := r.GetMark(ctx, mark)
	if err != nil {
		return err
	}
	return branch.ViewFS(ctx, func(mach *gotfs.Machine, stores stores.Reading, root gotfs.Root) error {
		return mach.ReadDir(ctx, stores, root, p, fn)
	})
}

func (r *Repo) Cat(ctx context.Context, mark FQM, p string, w io.Writer) error {
	branch, err := r.GetMark(ctx, mark)
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

func (r *Repo) Stat(ctx context.Context, mark FQM, p string) (*gotfs.Info, error) {
	branch, err := r.GetMark(ctx, mark)
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

// CheckAll runs integrity checks on all marks in the local Space.
func (r *Repo) CheckAll(ctx context.Context) error {
	return r.ForEachMark(ctx, "", func(name string) error {
		logctx.Infof(ctx, "checking mark %q", name)
		mark, err := r.GetMark(ctx, FQM{Name: name})
		if err != nil {
			return err
		}
		snap, tx, err := mark.GetTarget(ctx)
		if err != nil {
			return err
		}
		if snap == nil {
			return nil
		}
		defer tx.Abort(ctx)
		vcmach := mark.GotVC()
		return vcmach.Check(ctx, tx, *snap, func(payload marks.Payload) error {
			return mark.GotFS().Check(ctx, tx, payload.Root, func(ref gdat.Ref) error {
				return nil
			})
		})
	})
}
