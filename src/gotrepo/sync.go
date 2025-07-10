package gotrepo

import (
	"context"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/metrics"
)

// Sync synces 2 branches by name.
func (r *Repo) Sync(ctx context.Context, src, dst string, force bool) error {
	srcBranch, err := r.GetBranch(ctx, src)
	if err != nil {
		return err
	}
	dstBranch, err := r.GetBranch(ctx, dst)
	if err != nil {
		return err
	}
	ctx, cf := metrics.Child(ctx, "syncing volumes")
	defer cf()
	return branches.SyncVolumes(ctx, srcBranch.Volume, dstBranch.Volume, force)
}

type syncTask struct {
	Dst, Src string
}
