package gotcmd

import (
	"bufio"
	"fmt"

	"github.com/fatih/color"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(lsCmd)
	rootCmd.AddCommand(catCmd)
	rootCmd.AddCommand(scrubCmd)
}

var statusCmd = &cobra.Command{
	Use:     "status",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _, err := repo.GetActiveBranch(ctx)
		if err != nil {
			return err
		}
		bufw := bufio.NewWriter(cmd.OutOrStdout())
		fmt.Fprintf(bufw, "ACTIVE: %s\n", name)
		fmt.Fprintf(bufw, "STAGING:\n")
		if err := repo.ForEachStaging(ctx, func(p string, op gotrepo.Operation) error {
			var desc = "UNKNOWN"
			switch {
			case op.Delete:
				desc = color.RedString("DELETE")
			case op.Create != nil:
				desc = color.GreenString("CREATE")
			case op.Modify != nil:
				desc = color.YellowString("MODIFY")
			}
			fmt.Fprintf(bufw, "\t%7s %s\n", desc, p)
			return nil
		}); err != nil {
			return err
		}
		return bufw.Flush()
	},
}

var lsCmd = &cobra.Command{
	Use:     "ls",
	Short:   "lists the children of path in the current volume",
	PreRunE: loadRepo,
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

var catCmd = &cobra.Command{
	Use:     "cat",
	Short:   "writes the contents of path in the current volume to stdout",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		var p string
		if len(args) > 0 {
			p = args[0]
		}
		w := cmd.OutOrStdout()
		return repo.Cat(ctx, p, w)
	},
}

var scrubCmd = &cobra.Command{
	Use:     "scrub",
	Short:   "runs integrity checks on the current branch",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		return repo.Check(ctx)
	},
}
