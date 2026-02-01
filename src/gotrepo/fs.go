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

func (r *Repo) ViewFS(ctx context.Context, se gotcore.SnapExpr, fn func(fsmach *gotfs.Machine, s stores.Reading, root gotfs.Root) error) error {
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
			return fmt.Errorf("no snapshot found at %v", se)
		}
		snap, err := gotcore.GetSnapshot(ctx, ss[2], *ref)
		if err != nil {
			return err
		}
		fsmach := gotfs.NewMachine()
		s := st.Stores()
		return fn(fsmach, s[1], snap.Payload.Root)
	})
}

func (r *Repo) Ls(ctx context.Context, se gotcore.SnapExpr, p string, fn func(gotfs.DirEnt) error) error {
	return r.ViewFS(ctx, se, func(mach *gotfs.Machine, stores stores.Reading, root gotfs.Root) error {
		return mach.ReadDir(ctx, stores, root, p, fn)
	})
}

func (r *Repo) Cat(ctx context.Context, se gotcore.SnapExpr, p string, w io.Writer) error {
	return r.ViewFS(ctx, se, func(mach *gotfs.Machine, s stores.Reading, root gotfs.Root) error {
		fr, err := mach.NewReader(ctx, [2]stores.Reading{s, s}, root, p)
		if err != nil {
			return err
		}
		_, err = io.Copy(w, fr)
		return err
	})
}

func (r *Repo) Stat(ctx context.Context, se gotcore.SnapExpr, p string) (*gotfs.Info, error) {
	var info *gotfs.Info
	err := r.ViewFS(ctx, se, func(fsmach *gotfs.Machine, s stores.Reading, root gotfs.Root) error {
		var err error
		info, err = fsmach.GetInfo(ctx, s, root, p)
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
			se := &gotcore.SnapExpr_Mark{
				Space: "",
				Name:  name,
			}

			if err := gotcore.ViewSnapshot(ctx, st, se, func(vctx *gotcore.ViewCtx) error {
				return vctx.VC.Check(ctx, vctx.Stores[2], *vctx.Root, func(payload gotcore.Payload) error {
					return vctx.FS.Check(ctx, vctx.Stores[1], payload.Root, func(ref gdat.Ref) error {
						ok, err := stores.ExistsUnit(ctx, vctx.Stores[0], ref.CID)
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
