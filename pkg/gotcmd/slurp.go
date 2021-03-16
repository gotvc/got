package gotcmd

import (
	"github.com/brendoncarroll/got"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(slurpCmd)
}

var slurpCmd = &cobra.Command{
	Use:     "slurp",
	Short:   "imports a file or directory and returns a ref",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		store := repo.GetDefaultStore()
		ref, err := gotfs.New(ctx, store)
		if err != nil {
			return err
		}
		w := cmd.OutOrStdout()
		data, err := got.MarshalPEM(ref)
		if err != nil {
			return err
		}
		w.Write(data)
		return nil
	},
}
