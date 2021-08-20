package gotcmd

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

var (
	name      string
	readStdIn bool
)

func init() {
	rootCmd.AddCommand(branchCmd)
	rootCmd.AddCommand(activeCmd)
	rootCmd.AddCommand(forkCmd)

	branchCmd.AddCommand(createBranchCmd)
	branchCmd.AddCommand(listBranchCmd)
	branchCmd.AddCommand(deleteBranchCmd)
	branchCmd.AddCommand(getBranchHeadCmd)
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

var activeCmd = &cobra.Command{
	Use:      "active",
	Short:    "print the active branch or sets it",
	PreRunE:  loadRepo,
	PostRunE: closeRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			name, _, err := repo.GetActiveBranch(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), name)
			return nil
		}
		name := args[0]
		return repo.SetActiveBranch(ctx, name)
	},
}

var forkCmd = &cobra.Command{
	Use:      "fork",
	Short:    "creates a new branch based off the provided branch",
	PreRunE:  loadRepo,
	PostRunE: closeRepo,
	Args:     cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		newName := args[0]
		return repo.Fork(ctx, "", newName)
	},
}

var getBranchHeadCmd = &cobra.Command{
	Use:      "get-head",
	Short:    "prints the snapshot at the head of the provided branch",
	PreRunE:  loadRepo,
	PostRunE: closeRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		var name string
		if len(args) > 0 {
			name = args[0]

		}
		branchHead, err := repo.GetBranchHead(ctx, name)
		if err != nil {
			return err
		}
		return prettyPrintJSON(cmd.OutOrStdout(), branchHead)
	},
}

func prettyPrintJSON(w io.Writer, x interface{}) error {
	data, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}
