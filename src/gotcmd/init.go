package gotcmd

import (
	"fmt"

	"github.com/gotvc/got/src/gotrepo"
	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "initializes a repository in the current directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			config := gotrepo.DefaultConfig()
			if err := gotrepo.Init(".", config); err != nil {
				return err
			}
			w := cmd.ErrOrStderr()
			fmt.Fprintln(w, "successfully initialized got repo in current directory")
			return nil
		},
	}
}
