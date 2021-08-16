package gotcmd

import (
	"fmt"

	"github.com/gotvc/got/pkg/branches"
	"github.com/spf13/cobra"
)

var (
	name      string
	readStdIn bool
)

func init() {
	rootCmd.AddCommand(branchCmd)

	branchCmd.AddCommand(createBranchCmd)
	branchCmd.AddCommand(listBranchCmd)
	branchCmd.AddCommand(deleteBranchCmd)
}

var branchCmd = &cobra.Command{
	Use: "branch",
}

var createBranchCmd = &cobra.Command{
	Use:      "create",
	Short:    "creates a new branch",
	PreRunE:  loadRepo,
	PostRunE: closeRepo,
	Args:     cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		branchName := args[0]
		_, err := repo.CreateBranch(ctx, branchName)
		return err
	},
}

var setupVolCmd = &cobra.Command{
	Use:   "setup-vol",
	Short: "generates a volume spec of the specified type",
}

var listBranchCmd = &cobra.Command{
	Use:      "list",
	Short:    "lists the branches",
	PreRunE:  loadRepo,
	PostRunE: closeRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		r := repo.GetSpace()
		w := cmd.OutOrStdout()
		return r.ForEach(ctx, func(k string) error {
			fmt.Fprintf(w, "%s\n", k)
			return nil
		})
	},
}

var deleteBranchCmd = &cobra.Command{
	Use:      "delete",
	Short:    "deletes a branch",
	PreRunE:  loadRepo,
	PostRunE: closeRepo,
	Args:     cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		return repo.DeleteBranch(ctx, name)
	},
}

var switchCmd = &cobra.Command{
	Use:      "switch",
	Short:    "creates a local branch if it does not exist and switches to it",
	PreRunE:  loadRepo,
	PostRunE: closeRepo,
	Args:     cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var name string
		if _, err := repo.CreateBranch(ctx, name); err != nil && err != branches.ErrExists {
			return err
		}
		return repo.SetActiveBranch(ctx, name)
	},
}
