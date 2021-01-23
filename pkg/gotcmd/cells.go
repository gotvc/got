package gotcmd

import (
	"fmt"
	"io/ioutil"

	"github.com/brendoncarroll/got"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var cellName string

func init() {
	rootCmd.AddCommand(newCellCmd)
	// rootCmd.AddCommand(setupCellCmd)
	rootCmd.AddCommand(listCellCmd)
	rootCmd.AddCommand(branchCmd)

	newCellCmd.Flags().StringVar(&cellName, "name", "", "--name=cell-name")

	branchCmd.Flags().StringVar(&cellName, "name", "", "--name=cell-name")
}

var newCellCmd = &cobra.Command{
	Use:     "new-cell",
	Short:   "creates a cell with the config from stdin",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := cmd.ParseFlags(args); err != nil {
			return err
		}
		in := cmd.InOrStdin()
		data, err := ioutil.ReadAll(in)
		if err != nil {
			return err
		}
		spec, err := got.ParseCellSpec(data)
		if err != nil {
			return err
		}
		return repo.CreateCell(cellName, *spec)
	},
}

var setupCellCmd = &cobra.Command{
	Use:   "setup-cell",
	Short: "generates a cell spec of the specified type",
}

var listCellCmd = &cobra.Command{
	Use:     "ls-cell",
	Short:   "lists the cells",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		cs := repo.GetCellSpace()
		w := cmd.OutOrStdout()
		return cs.ForEach(ctx, "", func(name string) error {
			fmt.Fprintf(w, "%s\n", name)
			return nil
		})
	},
}

var rmCellCmd = &cobra.Command{
	Use:     "rm-cell",
	Short:   "deletes a cell",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		var cellName string
		if len(args[0]) > 0 {
			cellName = args[0]
		}
		if cellName == "" {
			return errors.Errorf("must specify cell name")
		}
		return repo.DeleteCell(ctx, cellName)
	},
}

var branchCmd = &cobra.Command{
	Use:     "branch",
	Short:   "creates a local cell and switches to it",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		var cellName string
		if len(args[0]) > 0 {
			cellName = args[0]
		}
		if cellName == "" {
			return errors.Errorf("must specify cell name")
		}
		if err := repo.CreateCell(cellName, got.CellSpec{Local: &got.LocalCellSpec{}}); err != nil {
			return err
		}
		return repo.SetActiveCell(ctx, cellName)
	},
}
