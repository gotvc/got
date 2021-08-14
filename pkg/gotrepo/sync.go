package gotrepo

import (
	"context"
)

// Sync synces 2 branches by name.
func (r *Repo) Sync(ctx context.Context, dst, src string, force bool) error {
	realm := r.GetSpace()
	srcBranch, err := realm.Get(ctx, src)
	if err != nil {
		return err
	}
	dstBranch, err := realm.Get(ctx, dst)
	if err != nil {
		return err
	}
	return syncVolumes(ctx, dstBranch.Volume, srcBranch.Volume, force)
}

type syncTask struct {
	Dst, Src string
}
