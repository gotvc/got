package gotcmd

import (
	"github.com/gotvc/got/pkg/gotrepo"
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
			var names []string
			if len(args) < 1 {
				names = []string{""}
			}
			for _, name := range names {
				if err := repo.CleanupBranch(ctx, name); err != nil {
					return err
				}
			}
			return nil
		},
	}
}
