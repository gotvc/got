package gotcmd

import (
	"fmt"
	"io"

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
		delta, err := repo.StagingDiff(ctx)
		if err != nil {
			return err
		}
		additions, err := delta.ListAdditionPaths(ctx, repo.StagingStore())
		if err != nil {
			return err
		}
		deletions, err := delta.ListDeletionPaths(ctx, repo.StagingStore())
		if err != nil {
			return err
		}

		fmt.Fprintf(w, "ACTIVE: %s\n", name)

		printToBeCommitted(w, additions, deletions)

		fmt.Fprintf(w, "Changes not staged for commit:\n")
		fmt.Fprintf(w, "  (use \"got add <file>...\" to update what will be commited)\n")
		fmt.Fprintf(w, "  (use \"got clobber <file>...\" to discard changes in working directory)\n")
		// TODO: list paths with staged versions
		fmt.Fprintln(w, "    < TODO >")

		fmt.Fprintf(w, "Untracked files:\n")
		fmt.Fprintf(w, "  (use \"got add <file>...\" to include what will be commited)\n")
		// TODO: list paths not in staging
		fmt.Fprintln(w, "    < TODO >")

		return nil
	},
}

func printToBeCommitted(w io.Writer, additions, deletions []string) {
	if len(additions) == 0 && len(deletions) == 0 {
		return
	}
	fmt.Fprintf(w, "Changes to be committed:\n")
	for _, p := range additions {
		fmt.Fprintf(w, "    modified: %s\n", p)
	}
	for _, p := range deletions {
		fmt.Fprintf(w, "    deleted: %s\n", p)
	}
}
