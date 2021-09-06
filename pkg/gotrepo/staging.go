package gotrepo

import (
	"context"
	"log"
	"time"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/fs"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"github.com/gotvc/got/pkg/porting"
	"github.com/gotvc/got/pkg/stores"
)

// SnapInfo is additional information that can be attached to a snapshot
type SnapInfo struct {
	Message   string
	CreatedAt *time.Time
}

func (r *Repo) Commit(ctx context.Context, snapInfo SnapInfo) error {
	if yes, err := r.tracker.IsEmpty(ctx); err != nil {
		return err
	} else if yes {
		log.Println("WARN: nothing to commit")
		return nil
	}
	_, branch, err := r.GetActiveBranch(ctx)
	if err != nil {
		return err
	}
	src := r.stagingTriple()
	dst := branch.Volume.StoreTriple()
	// writes go to src, but reads from src should fallback to dst
	src = branches.Triple{
		Raw: stores.AddWriteLayer(dst.Raw, src.Raw),
		FS:  stores.AddWriteLayer(dst.FS, src.FS),
		VC:  stores.AddWriteLayer(dst.VC, src.VC),
	}
	fsop := r.getFSOp(branch)
	err = branches.Apply(ctx, *branch, src, func(x *Snap) (*Snap, error) {
		y, err := gotvc.Change(ctx, src.VC, x, func(root *Root) (*Root, error) {
			wasEmpty := false
			if root == nil {
				wasEmpty = true
				if root, err = fsop.NewEmpty(ctx, src.FS); err != nil {
					return nil, err
				}
			}
			log.Println("begin processing tracked paths")
			if err := r.tracker.ForEach(ctx, func(target string) error {
				if !wasEmpty {
					if err := r.forEachToDelete(ctx, fsop, src.FS, *root, target, func(p string) error {
						var err error
						root, err = deletePath(ctx, fsop, src.FS, *root, r.workingDir, p)
						return err
					}); err != nil {
						return err
					}
				}
				if err := r.forEachToAdd(ctx, target, func(p string) error {
					root, err = r.putPath(ctx, fsop, src.FS, src.Raw, *root, r.workingDir, p)
					if err != nil {
						return err
					}
					return nil
				}); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return nil, err
			}
			log.Println("done processing tracked paths")
			return root, nil
		})
		if err != nil {
			return nil, err
		}
		y.CreatedAt = snapInfo.CreatedAt
		y.Message = snapInfo.Message
		return y, nil
	})
	if err != nil {
		return err
	}
	return r.tracker.Clear(ctx)
}

func (r *Repo) stagingStore() cadata.Store {
	return r.storeManager.GetStore(0)
}

func (r *Repo) stagingTriple() branches.Triple {
	return branches.Triple{
		VC:  r.stagingStore(),
		FS:  r.stagingStore(),
		Raw: r.stagingStore(),
	}
}

func (r *Repo) StagingStore() cadata.Store {
	return r.stagingStore()
}

func (r *Repo) forEachToDelete(ctx context.Context, fsop *gotfs.Operator, ms Store, root Root, target string, fn func(p string) error) error {
	return fsop.ForEach(ctx, ms, root, target, func(p string, md *gotfs.Metadata) error {
		exists, err := exists(r.workingDir, p)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		return fn(p)
	})
}

func (r *Repo) forEachToAdd(ctx context.Context, target string, fn func(p string) error) error {
	err := fs.WalkLeaves(ctx, r.workingDir, target, func(p string, _ fs.DirEnt) error {
		return fn(p)
	})
	if fs.IsErrNotExist(err) {
		err = nil
	}
	return err
}

func (r *Repo) putPath(ctx context.Context, fsop *gotfs.Operator, ms, ds Store, x Root, fsx fs.FS, p string) (*Root, error) {
	log.Println("processing PUT:", p)
	fileRoot, err := porting.ImportFile(ctx, fsop, ms, ds, fsx, p)
	if err != nil {
		return nil, err
	}
	return fsop.Graft(ctx, ms, x, p, *fileRoot)
}

// deletePath walks the path p in x and removes all the files which do not exist in fsx
func deletePath(ctx context.Context, fsop *gotfs.Operator, ms Store, x Root, fsx fs.FS, p string) (*Root, error) {
	y := &x
	err := fsop.ForEach(ctx, ms, x, p, func(p string, md *gotfs.Metadata) error {
		exists, err := exists(fsx, p)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		log.Println("processing DEL:", p)
		y, err = fsop.RemoveAll(ctx, ms, *y, p)
		return err
	})
	if err != nil {
		return nil, err
	}
	return y, nil
}
