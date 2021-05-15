package gotcmd

import "github.com/spf13/cobra"

func init() {
	rootCmd.AddCommand(cleanupCmd)
}

var cleanupCmd = &cobra.Command{
	Use:     "cleanup",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		return repo.Cleanup(ctx, args)
	},
}
