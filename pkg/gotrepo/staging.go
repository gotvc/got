package gotrepo

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/porting"
	"github.com/gotvc/got/pkg/staging"
	"github.com/gotvc/got/pkg/stores"
	"github.com/sirupsen/logrus"
)

func (r *Repo) Commit(ctx context.Context, snapInfo gotvc.SnapInfo) error {
	if yes, err := r.tracker.IsEmpty(ctx); err != nil {
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
		logrus.Println("begin processing tracked paths")
		nextRoot, err := r.applyTrackerChanges(ctx, fsop, src.FS, src.Raw, root)
		if err != nil {
			return nil, err
		}
		logrus.Println("done processing tracked paths")
		if err != nil {
			return nil, err
		}
		return vcop.NewSnapshot(ctx, src.VC, x, *nextRoot, snapInfo)
	})
	if err != nil {
		return err
	}
	if err := r.getStage().Reset(); err != nil {
		return err
	}
	return r.tracker.Clear(ctx)
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

// applyTrackerChanges iterates through all the tracked paths and adds or deletes them from root
// the new root, reflecting all of the changes indicated by the tracker, is returned.
func (r *Repo) applyTrackerChanges(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, root *Root) (*Root, error) {
	ads := stores.NewAsyncStore(ds, 32)
	porter := porting.NewPorter(fsop, r.workingDir, nil)
	stage := r.getStage()
	if err := r.tracker.ForEach(ctx, func(target string) error {
		root, err := porter.ImportPath(ctx, ms, ds, target)
		if err != nil {
			return err
		}
		return stage.Put(ctx, target, *root)
	}); err != nil {
		return nil, err
	}
	root, err := stage.Apply(ctx, fsop, ms, ds, root)
	if err != nil {
		return nil, err
	}
	if err := ads.Close(); err != nil {
		return nil, err
	}
	return root, nil
}

func (r *Repo) getStage() *staging.Stage {
	storage := staging.NewBoltStorage(r.db, bucketStaging)
	return staging.New(storage)
}
