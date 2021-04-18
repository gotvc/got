package gotcmd

import "github.com/spf13/cobra"

func init() {
	rootCmd.AddCommand(debugCmd)
}

var debugCmd = &cobra.Command{
	Use:     "debug",
	PreRunE: loadRepo,
	Hidden:  true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return repo.DebugDB(ctx, cmd.OutOrStdout())
	},
}
