package gotcmd

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(trackCmd)
	rootCmd.AddCommand(untrackCmd)
	rootCmd.AddCommand(clearCmd)
}

var trackCmd = &cobra.Command{
	Use:     "track",
	Short:   "tracks a path to be included in the next commit",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("path argument required")
		}
		p := args[0]
		return repo.Track(ctx, p)
	},
}

var untrackCmd = &cobra.Command{
	Use:     "untrack",
	Short:   "stops tracking a path to be included in the next commit",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.Errorf("path argument required")
		}
		p := args[0]
		return repo.Untrack(ctx, p)
	},
}

var clearCmd = &cobra.Command{
	Use:     "clear",
	Short:   "untracks all paths",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		return repo.Clear(ctx)
	},
}
