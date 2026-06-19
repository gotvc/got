package gotrepo

import "context"

// RepairRepoLinks refreshes repo-schema links in the repo volume.
func RepairRepoLinks(ctx context.Context, r *Repo) error {
	return r.repoc.RepairRepoLinks(ctx, r.rootVol)
}
