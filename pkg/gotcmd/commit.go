package gotcmd

import (
	"time"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(commitCmd)
}

var commitCmd = &cobra.Command{
	Use:     "commit",
	Short:   "commits the contents of staging applied to the contents of the active volume",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		now := time.Now()
		return repo.Commit(ctx, "", &now)
	},
}
