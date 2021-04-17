package gotcmd

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(addCmd)
	rootCmd.AddCommand(rmCmd)
}

var addCmd = &cobra.Command{
	Use:     "add",
	Short:   "adds a file to the staging tree",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("must provide path")
		}
		return repo.Add(ctx, args[0])
	},
}

var rmCmd = &cobra.Command{
	Use:     "rm",
	Short:   "removes a file from the staging tree",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("must provide path")
		}
		return repo.Remove(ctx, args[0])
	},
}

var unstageCmd = &cobra.Command{
	Use:     "unstage",
	Short:   "reverts the path in staging to its state from the active volume",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("must provide path")
		}
		return repo.Unstage(ctx, args[0])
	},
}
