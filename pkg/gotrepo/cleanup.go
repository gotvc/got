package gotrepo

import (
	"context"
	"log"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/branches"
)

func (r *Repo) Cleanup(ctx context.Context) error {
	return r.cleanupStaging(ctx)
}

func (r *Repo) cleanupStaging(ctx context.Context) error {
	s := r.stagingStore()
	return cadata.ForEach(ctx, r.stagingStore(), func(id cadata.ID) error {
		return s.Delete(ctx, id)
	})
}

func (r *Repo) CleanupBranch(ctx context.Context, name string) error {
	branch, err := r.GetBranch(ctx, name)
	if err != nil {
		return err
	}
	log.Println("begin cleanup on", name)
	if err := branches.CleanupVolume(ctx, branch.Volume); err != nil {
		return err
	}
	log.Println("done cleanup on", name)
	return nil
}
