package gotcmd

import "github.com/spf13/cobra"

func init() {
	rootCmd.AddCommand(debugCmd)
}

var debugCmd = &cobra.Command{
	Use:     "debug",
	PreRunE: loadRepo,
	Hidden:  true,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		p := args[0]
		switch p {
		case "db":
			return repo.DebugDB(ctx, cmd.OutOrStdout())
		case "fs":
			return repo.DebugFS(ctx, cmd.OutOrStdout())
		default:
			return nil
		}
	},
}
