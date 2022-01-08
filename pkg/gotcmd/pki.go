package gotcmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var localIDCmd = &cobra.Command{
	Use:     "local-id",
	Short:   "prints the local ID",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		id := repo.GetID()
		w := cmd.OutOrStdout()
		fmt.Fprintf(w, "%v\n", id)
		return nil
	},
}
