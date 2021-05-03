package got

import (
	"context"
	"log"
	"time"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/brendoncarroll/got/pkg/gotvc"
)

func (r *Repo) Commit(ctx context.Context, message string, createdAt *time.Time) error {
	if yes, err := r.tracker.IsEmpty(ctx); err != nil {
		return err
	} else if yes {
		log.Println("WARN: nothing to commit")
		return nil
	}
	log.Println("staging changes...")
	stage := gotvc.NewStage(cells.NewMem(), r.stagingStore(), r.stagingStore(), r.getFSOp())
	// add all the paths tracked by the tracker to the store
	if err := r.tracker.ForEach(ctx, func(p string) error {
		log.Println("resolving", p)
		err := fs.WalkFiles(ctx, r.workingDir, p, func(p string) error {
			rc, err := r.workingDir.Open(p)
			if err != nil && !fs.IsNotExist(err) {
				return err
			}
			defer rc.Close()
			log.Println("processing ADD:", p)
			return stage.Add(ctx, p, rc)
		})
		if fs.IsNotExist(err) {
			log.Println("processing DEL:", p)
			return stage.Remove(ctx, p)
		}
		return err
	}); err != nil {
		return err
	}
	log.Println("done staging changes")

	_, vol, err := r.GetActiveVolume(ctx)
	if err != nil {
		return err
	}
	err = applySnapshot(ctx, vol.Cell, func(x *Commit) (*Commit, error) {
		y, err := stage.Snapshot(ctx, x, message, createdAt)
		if err != nil {
			return nil, err
		}
		dst := tripleFromVolume(*vol)
		src := r.stagingTriple()
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
