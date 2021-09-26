package gotcmd

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(addCmd)
	rootCmd.AddCommand(discardCmd)
	rootCmd.AddCommand(clearCmd)
}

var addCmd = &cobra.Command{
	Use:     "add",
	Short:   "adds a path to the staging area",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("path argument required")
		}
		p := args[0]
		return repo.Track(ctx, p)
	},
}

var discardCmd = &cobra.Command{
	Use:     "discard",
	Short:   "discards any changes staged for a path",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("path argument required")
		}
		return repo.Discard(ctx, args[1:]...)
	},
}

var clearCmd = &cobra.Command{
	Use:     "clear",
	Short:   "clears the staging area",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		return repo.Clear(ctx)
	},
}
