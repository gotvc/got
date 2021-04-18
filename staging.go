package got

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"time"

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

func (r *Repo) Commit(ctx context.Context, message string, createdAt *time.Time) error {
	if yes, err := r.StagingIsEmpty(ctx); err != nil {
		return err
	} else if yes {
		log.Println("WARN: nothing to commit")
		return nil
	}
	_, vol, err := r.GetActiveVolume(ctx)
	if err != nil {
		return err
	}
	stage := r.stage()
	err = applySnapshot(ctx, vol.Cell, func(x *Commit) (*Commit, error) {
		y, err := stage.Snapshot(ctx, x, message, createdAt)
		if err != nil {
			return nil, err
		}
		if y.Parent != nil {
			if err := gotvc.Copy(ctx, vol.Store, r.stagingStore(), *y.Parent); err != nil {
				return nil, err
			}
		}
		if err := gotfs.Copy(ctx, vol.Store, r.stagingStore(), y.Root); err != nil {
			return nil, err
		}
		return y, nil
	})
	if err != nil {
		return err
	}
	return stage.Clear(ctx)
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
