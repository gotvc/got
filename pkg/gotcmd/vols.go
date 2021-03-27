package gotcmd

import (
	"fmt"
	"io/ioutil"

	"github.com/brendoncarroll/got"
	"github.com/brendoncarroll/got/pkg/realms"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var cellName string

func init() {
	rootCmd.AddCommand(newVolCmd)
	// rootCmd.AddCommand(setupVolCmd)
	rootCmd.AddCommand(listVolCmd)
	rootCmd.AddCommand(rmVolCmd)
	rootCmd.AddCommand(branchCmd)

	newVolCmd.Flags().StringVar(&cellName, "name", "", "--name=vol-name")
}

var newVolCmd = &cobra.Command{
	Use:     "new-vol",
	Short:   "creates a volume with the config from stdin",
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
		cellSpec, err := got.ParseCellSpec(data)
		if err != nil {
			return err
		}
		spec := got.VolumeSpec{
			Cell:  *cellSpec,
			Store: got.StoreSpec{Local: &got.LocalStoreSpec{}},
		}
		return repo.CreateVolume(cellName, spec)
	},
}

var setupVolCmd = &cobra.Command{
	Use:   "setup-vol",
	Short: "generates a volume spec of the specified type",
}

var listVolCmd = &cobra.Command{
	Use:     "ls-vol",
	Short:   "lists the volumes",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		r := repo.GetRealm()
		w := cmd.OutOrStdout()
		return realms.ForEach(ctx, r, func(k string) error {
			fmt.Fprintf(w, "%s\n", k)
			return nil
		})
	},
}

var rmVolCmd = &cobra.Command{
	Use:     "rm-vol",
	Short:   "deletes a volume",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		var cellName string
		if len(args[0]) > 0 {
			cellName = args[0]
		}
		if cellName == "" {
			return errors.Errorf("must specify cell name")
		}
		return repo.DeleteVolume(ctx, cellName)
	},
}

var branchCmd = &cobra.Command{
	Use:     "branch",
	Short:   "creates a local volume if it does not exist and switches to it",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		var name string
		if len(args) > 0 {
			name = args[0]
		}
		// if name is empty then print the active volume
		if name == "" {
			name, _, err := repo.GetActiveVolume(ctx)
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "ACTIVE VOLUME: %s\n", name)
			return nil
		}

		spec := got.VolumeSpec{
			Cell:  got.CellSpec{Local: &got.LocalCellSpec{}},
			Store: got.StoreSpec{Local: &got.LocalStoreSpec{}},
		}
		if err := repo.CreateVolume(name, spec); err != nil && err != realms.ErrExists {
			return err
		}
		return repo.SetActiveVolume(ctx, name)
	},
}
