package gotcmd

import (
	"fmt"

	"github.com/brendoncarroll/got"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(initCmd)
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "initializes a repository in the current directory",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := got.InitRepo("."); err != nil {
			return err
		}
		w := cmd.ErrOrStderr()
		fmt.Fprintf(w, "successfully initialized got repo in current directory")
		return nil
	},
}
