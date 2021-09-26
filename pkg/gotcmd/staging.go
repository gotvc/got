package gotcmd

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(addCmd)
	rootCmd.AddCommand(rmCmd)
	rootCmd.AddCommand(discardCmd)
	rootCmd.AddCommand(clearCmd)
}

var addCmd = &cobra.Command{
	Use:     "add",
	Short:   "adds paths to the staging area",
	PreRunE: loadRepo,
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("path argument required")
		}
		return repo.Add(ctx, args...)
	},
}

var rmCmd = &cobra.Command{
	Use:     "rm",
	Short:   "stages paths for deletion",
	PreRunE: loadRepo,
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("path argument required")
		}
		return repo.Rm(ctx, args...)
	},
}

var putCmd = &cobra.Command{
	Use:     "put",
	Short:   "stages paths for replacement",
	PreRunE: loadRepo,
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("path argument required")
		}
		return repo.Put(ctx, args...)
	},
}

var discardCmd = &cobra.Command{
	Use:   "discard",
	Short: "discards any changes staged for a path",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("path argument required")
		}
		return repo.Discard(ctx, args...)
	},
}

var clearCmd = &cobra.Command{
	Use:     "clear",
	Short:   "clears the staging area",
	PreRunE: loadRepo,
	Args:    cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		return repo.Clear(ctx)
	},
}
