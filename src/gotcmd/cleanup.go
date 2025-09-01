package gotcmd

import (
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/spf13/cobra"
)

func newCleanupCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "cleanup",
		Short:    "cleanup cleans up unreferenced data associated with branches",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cf := metrics.Child(ctx, "cleanup")
			defer cf()
			return repo.Cleanup(ctx)
		},
	}
}
