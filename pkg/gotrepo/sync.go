package gotrepo

import (
	"context"

	"github.com/gotvc/got/pkg/branches"
)

// Sync synces 2 branches by name.
func (r *Repo) Sync(ctx context.Context, dst, src string, force bool) error {
	dstBranch, err := r.GetBranch(ctx, dst)
	if err != nil {
		return err
	}
	srcBranch, err := r.GetBranch(ctx, src)
	if err != nil {
		return err
	}
	return branches.SyncVolumes(ctx, dstBranch.Volume, srcBranch.Volume, force)
}

type syncTask struct {
	Dst, Src string
}
