package gotrepo

import (
	"context"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/logctx"
)

// Sync synces 2 branches by name.
func (r *Repo) Sync(ctx context.Context, src, dst string, force bool) error {
	logctx.Infof(ctx, "syncing %q to %q", src, dst)
	srcBranch, err := r.GetBranch(ctx, src)
	if err != nil {
		return err
	}
	dstBranch, err := r.GetBranch(ctx, dst)
	if err != nil {
		return err
	}
	return branches.SyncVolumes(ctx, srcBranch.Volume, dstBranch.Volume, force)
}

type syncTask struct {
	Dst, Src string
}
