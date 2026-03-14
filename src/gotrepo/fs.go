package gotrepo

import (
	"context"
	"fmt"
	"io"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/stdctx/logctx"
)

func (r *Repo) ViewFS(ctx context.Context, se gotcore.CommitExpr, fn func(fsmach *gotfs.Machine, s gotfs.RO, root gotfs.Root) error) error {
	sp, err := r.GetSpace(ctx, se.GetSpace())
	if err != nil {
		return err
	}
	return sp.Do(ctx, false, func(st gotcore.SpaceTx) error {
		ref, err := se.Resolve(ctx, st)
		if err != nil {
			return err
		}
		ss := st.Stores()
		if ref.IsZero() {
			return fmt.Errorf("no commit found at %v", se)
		}
		comm, err := gotcore.GetCommit(ctx, ss.VC, ref)
		if err != nil {
			return err
		}
		fsmach := gotfs.NewMachine(gotfs.Params{})
		s := st.Stores()
		return fn(&fsmach, s.FS.RO(), comm.Payload.Snap)
	})
}

func (r *Repo) Ls(ctx context.Context, se gotcore.CommitExpr, p string, fn func(gotfs.DirEnt) error) error {
	return r.ViewFS(ctx, se, func(mach *gotfs.Machine, stores gotfs.RO, root gotfs.Root) error {
		return mach.ReadDir(ctx, stores.Metadata, root, p, fn)
	})
}

func (r *Repo) Cat(ctx context.Context, se gotcore.CommitExpr, p string, w io.Writer) error {
	return r.ViewFS(ctx, se, func(mach *gotfs.Machine, s gotfs.RO, root gotfs.Root) error {
		fr, err := mach.NewReader(ctx, s, root, p)
		if err != nil {
			return err
		}
		_, err = io.Copy(w, fr)
		return err
	})
}

func (r *Repo) Stat(ctx context.Context, se gotcore.CommitExpr, p string) (*gotfs.Info, error) {
	var info *gotfs.Info
	err := r.ViewFS(ctx, se, func(fsmach *gotfs.Machine, s gotfs.RO, root gotfs.Root) error {
		var err error
		info, err = fsmach.GetInfo(ctx, s.Metadata, root, p)
		return err
	})
	return info, err
}

// CheckAll runs integrity checks on all marks in the local Space.
func (r *Repo) CheckAll(ctx context.Context) error {
	sp, err := r.GetSpace(ctx, "")
	if err != nil {
		return err
	}
	return sp.Do(ctx, false, func(st gotcore.SpaceTx) error {
		for name, err := range st.All(ctx) {
			if err != nil {
				return err
			}
			logctx.Infof(ctx, "checking mark %q", name)
			se := &gotcore.CommitExpr_Mark{
				Space: "",
				Name:  name,
			}

			if err := gotcore.ViewCommit(ctx, st, se, func(vctx *gotcore.ViewCtx) error {
				return vctx.VC.Check(ctx, vctx.Stores.VC, *vctx.Root, func(payload gotcore.Payload) error {
					return vctx.FS.Check(ctx, vctx.Stores.FS.Metadata, payload.Snap, func(ref gdat.Ref) error {
						ok, err := stores.ExistsUnit(ctx, vctx.Stores.FS.Data, ref.CID)
						if err != nil {
							return err
						}
						if !ok {
							return fmt.Errorf("dangling reference to %v", ref)
						}
						return nil
					})
				})
			}); err != nil {
				return err
			}
		}
		return nil
	})
}
