package gotrepo

import (
	"context"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/metrics"
)

// Sync syncs 2 branches by name.
func (r *Repo) Sync(ctx context.Context, src, dst string, force bool) error {
	srcBranch, err := r.GetMark(ctx, src)
	if err != nil {
		return err
	}
	dstBranch, err := r.GetMark(ctx, dst)
	if err != nil {
		return err
	}
	ctx, cf := metrics.Child(ctx, "syncing volumes")
	defer cf()
	return branches.Sync(ctx, srcBranch, dstBranch, force)
}

type syncTask struct {
	Dst, Src string
}
