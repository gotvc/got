package got

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotvc"
)

func (r *Repo) ApplyStaging(ctx context.Context, fn func(s Store, x Root) (*gotvc.Root, error)) error {
	store := r.stagingStore()
	cell := r.stagingCell()
	return cells.Apply(ctx, cell, func(data []byte) ([]byte, error) {
		var x Root
		var err error
		if len(data) > 0 {
			if err := json.Unmarshal(data, &x); err != nil {
				return nil, err
			}
		} else {
			// no root in staging cell, need to generate.
			_, vol, err := r.GetActiveVolume(ctx)
			if err != nil {
				return nil, err
			}
			root, err := r.getRootFromVolume(ctx, *vol)
			if err != nil {
				return nil, err
			}
			if root == nil {
				if root, err = r.createEmptyRoot(ctx, store); err != nil {
					return nil, err
				}
			}
			x = *root
		}
		y, err := fn(store, x)
		if err != nil {
			return nil, err
		}
		return json.Marshal(y)
	})
}

func (r *Repo) Add(ctx context.Context, p string) error {
	finfo, err := r.workingDir.Stat(p)
	if err != nil {
		return err
	}
	if finfo.IsDir() {
		return r.workingDir.ReadDir(p, func(finfo os.FileInfo) error {
			p2 := filepath.Join(p, finfo.Name())
			return r.Add(ctx, p2)
		})
	}
	fsop := r.getFSOp()
	return r.ApplyStaging(ctx, func(s Store, x Root) (*Root, error) {
		rc, err := r.workingDir.Open(p)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		root := &x
		root, err = fsop.RemoveAll(ctx, s, *root, p)
		if err != nil {
			return nil, err
		}
		return r.getFSOp().CreateFile(ctx, s, *root, p, rc)
	})
}

func (r *Repo) Remove(ctx context.Context, p string) error {
	return r.ApplyStaging(ctx, func(s Store, x Root) (*Root, error) {
		return r.getFSOp().RemoveAll(ctx, s, x, p)
	})
}

func (r *Repo) Unstage(ctx context.Context, p string) error {
	panic("not implemented")
}

func (r *Repo) StagingIsEmpty(ctx context.Context) (bool, error) {
	cell := r.stagingCell()
	data, err := cell.Get(ctx)
	if err != nil {
		return false, err
	}
	return len(data) == 0, nil
}

func (r *Repo) Commit(ctx context.Context) error {
	_, vol, err := r.GetActiveVolume(ctx)
	if err != nil {
		return err
	}
	rootData, err := r.stagingCell().Get(ctx)
	if err != nil {
		return err
	}
	var root gotfs.Root
	if err := json.Unmarshal(rootData, &root); err != nil {
		return err
	}
	if err := gotfs.Copy(ctx, vol.Store, r.stagingStore(), root); err != nil {
		return err
	}
	err = ApplyRef(ctx, vol.Cell, func(ref *Ref) (*Ref, error) {
		snap, err := gotvc.NewSnapshot(ctx, vol.Store, root, ref)
		if err != nil {
			return nil, err
		}
		ref, err = gotvc.PostSnapshot(ctx, r.stagingStore(), *snap)
		if err != nil {
			return nil, err
		}
		if err := cadata.Copy(ctx, vol.Store, r.stagingStore(), ref.CID); err != nil {
			return nil, err
		}
		return ref, nil
	})
	if err != nil {
		return err
	}
	return r.ClearStaging(ctx)
}

func (r *Repo) ClearStaging(ctx context.Context) error {
	return cells.Apply(ctx, r.stagingCell(), func([]byte) ([]byte, error) {
		return nil, nil
	})
}

func (r *Repo) StagingDiff(ctx context.Context, addFn, delFn func(string)) error {
	rootData, err := r.stagingCell().Get(ctx)
	if err != nil {
		return err
	}
	var stagingRoot Root
	if err := json.Unmarshal(rootData, &stagingRoot); err != nil {
		return err
	}
	_, vol, err := r.GetActiveVolume(ctx)
	if err != nil {
		return err
	}
	prevRoot, err := r.getRootFromVolume(ctx, *vol)
	if err != nil {
		return err
	}
	if prevRoot == nil {
		if prevRoot, err = r.createEmptyRoot(ctx, r.StagingStore()); err != nil {
			return err
		}
	}
	return r.getFSOp().DiffPaths(ctx, r.stagingStore(), *prevRoot, stagingRoot, addFn, delFn)
}

func (r *Repo) stagingStore() cadata.Store {
	return r.storeManager.GetStore(0)
}

func (r *Repo) stagingCell() cells.Cell {
	return cells.NewBoltCell(r.db, []string{bucketDefault, keyStaging})
}

func (r *Repo) StagingStore() cadata.Store {
	return r.stagingStore()
}
