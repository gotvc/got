package gotrepo

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/porting"
	"github.com/gotvc/got/pkg/staging"
	"github.com/gotvc/got/pkg/stores"
	"github.com/sirupsen/logrus"
)

func (r *Repo) Track(ctx context.Context, paths ...string) error {
	branch, err := r.GetBranch(ctx, "")
	if err != nil {
		return err
	}
	storeTriple := r.stagingTriple()
	ms := storeTriple.FS
	ds := storeTriple.Raw
	porter := porting.NewPorter(r.getFSOp(branch), r.workingDir, nil)
	stage := r.stage
	for _, p := range paths {
		root, err := porter.ImportPath(ctx, ms, ds, p)
		if err != nil && !posixfs.IsErrNotExist(err) {
			return err
		}
		if posixfs.IsErrNotExist(err) {
			if err := stage.Delete(ctx, p); err != nil {
				return err
			}
		} else {
			if err := stage.Put(ctx, p, *root); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Repo) Discard(ctx context.Context, paths ...string) error {
	for _, p := range paths {
		if err := r.stage.Discard(ctx, p); err != nil {
			return err
		}
	}
	return nil
}

func (r Repo) Clear(ctx context.Context) error {
	return r.stage.Reset()
}

func (r *Repo) ForEachStaging(ctx context.Context, fn func(p string, fo staging.FileOp) error) error {
	return r.stage.ForEach(ctx, fn)
}

func (r *Repo) Commit(ctx context.Context, snapInfo gotvc.SnapInfo) error {
	if yes, err := r.stage.IsEmpty(ctx); err != nil {
		return err
	} else if yes {
		logrus.Warn("nothing to commit")
		return nil
	}
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	snapInfo.Creator = r.GetID()
	src := r.stagingTriple()
	dst := branch.Volume.StoreTriple()
	// writes go to src, but reads from src should fallback to dst
	src = branches.StoreTriple{
		Raw: stores.AddWriteLayer(dst.Raw, src.Raw),
		FS:  stores.AddWriteLayer(dst.FS, src.FS),
		VC:  stores.AddWriteLayer(dst.VC, src.VC),
	}
	fsop := r.getFSOp(branch)
	vcop := r.getVCOp(branch)
	err = branches.Apply(ctx, *branch, src, func(x *Snap) (*Snap, error) {
		var root *Root
		if x != nil {
			root = &x.Root
		}
		logrus.Println("begin applying staged changes")
		nextRoot, err := r.stage.Apply(ctx, fsop, src.FS, src.Raw, root)
		if err != nil {
			return nil, err
		}
		logrus.Println("done applying staged changes")
		return vcop.NewSnapshot(ctx, src.VC, x, *nextRoot, snapInfo)
	})
	if err != nil {
		return err
	}
	return r.getStage().Reset()
}

func (r *Repo) stagingStore() cadata.Store {
	return r.storeManager.GetStore(0)
}

func (r *Repo) stagingTriple() branches.StoreTriple {
	return branches.StoreTriple{
		VC:  r.stagingStore(),
		FS:  r.stagingStore(),
		Raw: r.stagingStore(),
	}
}

func (r *Repo) StagingStore() cadata.Store {
	return r.stagingStore()
}

func (r *Repo) getStage() *staging.Stage {
	storage := staging.NewBoltStorage(r.db, bucketStaging)
	return staging.New(storage)
}
