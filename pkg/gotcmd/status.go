package gotcmd

import (
	"fmt"

	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(lsCmd)
	rootCmd.AddCommand(catCmd)
	rootCmd.AddCommand(checkCmd)
}

var statusCmd = &cobra.Command{
	Use:     "status",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		w := cmd.OutOrStdout()
		name, _, err := repo.GetActiveBranch(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "ACTIVE: %s\n", name)
		fmt.Fprintf(w, "TRACKED PATHS:\n")
		return repo.ForEachTracked(ctx, func(p string, isDelete bool) error {
			desc := "ADD"
			if isDelete {
				desc = "DEL"
			}
			fmt.Fprintf(w, "\t%s %s\n", desc, p)
			return nil
		})
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

var checkCmd = &cobra.Command{
	Use:     "check",
	Short:   "checks contents of the current volume",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		return repo.Check(ctx)
	},
}
