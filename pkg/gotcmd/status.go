package gotcmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(statusCmd)
}

var statusCmd = &cobra.Command{
	Use:     "status",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		w := cmd.OutOrStdout()
		name, _, err := repo.GetActiveVolume(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "Active Cell: %s\n", name)

		fmt.Fprintf(w, "Changes not staged for commit:\n")
		fmt.Fprintf(w, "  (use \"got add <file>...\" to update what will be commited)\n")
		fmt.Fprintf(w, "  (use \"got export <file>...\" to discard changes in working directory)\n")
		// TODO: list paths with staged versions

		fmt.Fprintf(w, "Untracked files:\n")
		fmt.Fprintf(w, "  (use \"got add <file>...\" to include what will be commited)\n")
		// TODO: list paths not in staging

		return nil
	},
}
