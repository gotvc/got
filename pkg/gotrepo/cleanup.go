package gotrepo

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

func (r *Repo) Cleanup(ctx context.Context) error {
	return r.cleanupStaging(ctx)
}

func (r *Repo) cleanupStaging(ctx context.Context) error {
	s := r.stagingStore()
	return cadata.ForEach(ctx, r.stagingStore(), cadata.Span{}, func(id cadata.ID) error {
		return s.Delete(ctx, id)
	})
}
