package gotrepo

import (
	"context"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/metrics"
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
	ctx = metrics.Child(ctx, "syncing volumes")
	defer metrics.Close(ctx)
	return branches.SyncVolumes(ctx, srcBranch.Volume, dstBranch.Volume, force)
}

type syncTask struct {
	Dst, Src string
}
