package got

import (
	"context"
)

// Sync synces 2 volumes by name.
func (r *Repo) Sync(ctx context.Context, dst, src string, force bool) error {
	realm := r.GetRealm()
	srcV, err := realm.Get(ctx, src)
	if err != nil {
		return err
	}
	dstV, err := realm.Get(ctx, dst)
	if err != nil {
		return err
	}
	return SyncVolumes(ctx, *dstV, *srcV, force)
}

type syncTask struct {
	Dst, Src string
}
