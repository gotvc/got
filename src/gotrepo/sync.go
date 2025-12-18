package gotrepo

import (
	"context"

	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/marks"
)

// Sync syncs 2 branches by name.
func (r *Repo) Sync(ctx context.Context, src, dst FQM, force bool) error {
	srcSpace, err := r.GetSpace(ctx, src.Space)
	if err != nil {
		return err
	}
	dstSpace, err := r.GetSpace(ctx, dst.Space)
	if err != nil {
		return err
	}
	srcBranch, err := srcSpace.Open(ctx, src.Name)
	if err != nil {
		return err
	}
	dstBranch, err := dstSpace.Open(ctx, dst.Name)
	if err != nil {
		return err
	}
	ctx, cf := metrics.Child(ctx, "syncing volumes")
	defer cf()
	return marks.Sync(ctx, srcBranch, dstBranch, force)
}

type syncTask struct {
	Dst, Src string
}
