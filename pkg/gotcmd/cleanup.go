package gotcmd

import "github.com/spf13/cobra"

var cleanupCmd = &cobra.Command{
	Use:      "cleanup",
	Short:    "cleanup cleans up unreferenced data associated with branches",
	PreRunE:  loadRepo,
	PostRunE: closeRepo,
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
