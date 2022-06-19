package gotcmd

import (
	"bufio"
	"fmt"

	"github.com/fatih/color"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/spf13/cobra"
)

func newStatusCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "status",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _, err := repo.GetActiveBranch(ctx)
			if err != nil {
				return err
			}
			bufw := bufio.NewWriter(cmd.OutOrStdout())
			if _, err := fmt.Fprintf(bufw, "BRANCH: %s\n", name); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(bufw, "STAGED:\n"); err != nil {
				return err
			}
			if err := repo.ForEachStaging(ctx, func(p string, op gotrepo.FileOperation) error {
				var desc = "UNKNOWN"
				switch {
				case op.Delete:
					desc = color.RedString("DELETE")
				case op.Create != nil:
					desc = color.BlueString("CREATE")
				case op.Modify != nil:
					desc = color.GreenString("MODIFY")
				}
				_, err := fmt.Fprintf(bufw, "\t%7s %s\n", desc, p)
				return err
			}); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(bufw, "UNTRACKED:\n"); err != nil {
				return err
			}
			if err := repo.ForEachUntracked(ctx, func(p string) error {
				_, err := fmt.Fprintf(bufw, "\t%s\n", p)
				return err
			}); err != nil {
				return err
			}
			return bufw.Flush()
		},
	}
}

func newLsCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "ls",
		Short:    "lists the children of path in the current volume",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		RunE: func(cmd *cobra.Command, args []string) error {
			var p string
			if len(args) > 0 {
				p = args[0]
			}
			w := cmd.OutOrStdout()
			return repo.Ls(ctx, p, func(ent gotfs.DirEnt) error {
				_, err := fmt.Fprintf(w, "%v %s\n", ent.Mode, ent.Name)
				return err
			})
		},
	}
}

func newCatCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "cat",
		Short:    "writes the contents of path in the current volume to stdout",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		RunE: func(cmd *cobra.Command, args []string) error {
			var p string
			if len(args) > 0 {
				p = args[0]
			}
			w := cmd.OutOrStdout()
			return repo.Cat(ctx, p, w)
		},
	}
}

func newScrubCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "scrub",
		Short:    "runs integrity checks on the current branch",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		RunE: func(cmd *cobra.Command, args []string) error {
			return repo.Check(ctx)
		},
	}
}
