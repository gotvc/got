package gotcmd

import (
	"fmt"

	"github.com/gotvc/got"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "initializes a repository in the current directory",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := got.InitRepo("."); err != nil {
			return err
		}
		w := cmd.ErrOrStderr()
		fmt.Fprintln(w, "successfully initialized got repo in current directory")
		return nil
	},
}
