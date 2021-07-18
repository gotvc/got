package gotrepo

import (
	"context"
	"log"
	"time"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/fs"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
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
	vol := branch.Volume
	err = applySnapshot(ctx, vol.Cell, func(x *Snap) (*Snap, error) {
		dst := tripleFromVolume(vol)
		src := r.stagingTriple()

		y, err := gotvc.Change(ctx, src.VC, x, func(root *Root) (*Root, error) {
			wasEmpty := false
			if root == nil {
				wasEmpty = true
				if root, err = r.getFSOp().NewEmpty(ctx, src.FS); err != nil {
					return nil, err
				}
			} else {
				// reverse src, and dst here
				if err := gotfs.Sync(ctx, src.FS, dst.FS, *root, func(gdat.Ref) error { return nil }); err != nil {
					return nil, err
				}
			}
			log.Println("begin processing tracked paths")
			if err := r.tracker.ForEach(ctx, func(target string) error {
				if !wasEmpty {
					if err := r.forEachToDelete(ctx, src.FS, *root, target, func(p string) error {
						var err error
						root, err = r.deletePath(ctx, src.FS, *root, r.workingDir, p)
						return err
					}); err != nil {
						return err
					}
				}
				if err := r.forEachToAdd(ctx, target, func(p string) error {
					root, err = r.putPath(ctx, src.FS, src.Raw, *root, p)
					if err != nil {
						panic(err)
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
		if err := syncStores(ctx, dst, src, *y); err != nil {
			return nil, err
		}
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

func (r *Repo) stagingTriple() triple {
	return triple{VC: r.stagingStore(), FS: r.stagingStore(), Raw: r.stagingStore()}
}

func (r *Repo) StagingStore() cadata.Store {
	return r.stagingStore()
}

func (r *Repo) forEachToDelete(ctx context.Context, ms Store, root Root, target string, fn func(p string) error) error {
	return r.getFSOp().ForEach(ctx, ms, root, target, func(p string, md *gotfs.Metadata) error {
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

func (r *Repo) putPath(ctx context.Context, ms, ds Store, x Root, p string) (*Root, error) {
	log.Println("processing PUT:", p)
	fileRoot, err := r.porter.Import(ctx, ms, ds, r.repoFS, p)
	if err != nil {
		return nil, err
	}
	return r.getFSOp().Graft(ctx, ms, x, p, *fileRoot)
}

// deletePath walks the path p in x and removes all the files which do not exist in fsx
func (r *Repo) deletePath(ctx context.Context, ms Store, x Root, fsx fs.FS, p string) (*Root, error) {
	fsop := r.getFSOp()
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
