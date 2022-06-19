package gotcmd

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/spf13/cobra"
)

func newBranchCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	bc := &cobra.Command{
		Use: "branch",
	}
	var createBranchCmd = &cobra.Command{
		Use:      "create",
		Short:    "creates a new branch",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		Args:     cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			branchName := args[0]
			_, err := repo.CreateBranch(ctx, branchName, branches.NewParams(false))
			return err
		},
	}
	var listBranchCmd = &cobra.Command{
		Use:      "list",
		Short:    "lists the branches",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		RunE: func(cmd *cobra.Command, args []string) error {
			w := cmd.OutOrStdout()
			return repo.ForEachBranch(ctx, func(k string) error {
				fmt.Fprintf(w, "%s\n", k)
				return nil
			})
		},
	}
	var deleteBranchCmd = &cobra.Command{
		Use:      "delete",
		Short:    "deletes a branch",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		Args:     cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			return repo.DeleteBranch(ctx, name)
		},
	}
	var getBranchHeadCmd = &cobra.Command{
		Use:      "get-head",
		Short:    "prints the snapshot at the head of the provided branch",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
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
	for _, c := range []*cobra.Command{
		createBranchCmd,
		listBranchCmd,
		deleteBranchCmd,
		getBranchHeadCmd,
	} {
		bc.AddCommand(c)
	}
	return bc
}

func newActiveCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "active",
		Short:    "print the active branch or sets it",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
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
}

func newForkCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "fork",
		Short:    "creates a new branch based off the provided branch",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		Args:     cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			newName := args[0]
			return repo.Fork(ctx, "", newName)
		},
	}
}

func prettyPrintJSON(w io.Writer, x interface{}) error {
	data, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}
