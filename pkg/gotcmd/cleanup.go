package gotcmd

import "github.com/spf13/cobra"

func init() {
	rootCmd.AddCommand(cleanupCmd)
}

var cleanupCmd = &cobra.Command{
	Use:      "cleanup",
	Short:    "cleanup cleans up unreferenced data associated with branches",
	PreRunE:  loadRepo,
	PostRunE: closeRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		return repo.CleanupBranches(ctx, args)
	},
}
