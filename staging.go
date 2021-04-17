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
	rc, err := r.workingDir.Open(p)
	if err != nil {
		return err
	}
	defer rc.Close()
	return r.stage().Add(ctx, p, rc)	
}

func (r *Repo) Remove(ctx context.Context, p string) error {
	return r.stage().Remove(ctx, p)
}

func (r *Repo) Unstage(ctx context.Context, p string) error {
	return r.stage().Unstage(ctx, p)
}

func (r *Repo) StagingIsEmpty(ctx context.Context) (bool, error) {
	cell := r.stagingCell()
	data, err := cell.Get(ctx)
	if err != nil {
		return false, err
	}
	return len(data) == 0, nil
}

func (r *Repo) ClearStaging(ctx context.Context) error {
	return r.stage().Clear(ctx)
}

func (r *Repo) StagingDiff(ctx context.Context) (*gotvc.Delta, error) {
	return r.stage().Delta(ctx)
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

func (r *Repo) stage() *gotvc.Stage {
	return gotvc.NewStage(r.stagingCell(), r.stagingStore(), r.getFSOp())
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
