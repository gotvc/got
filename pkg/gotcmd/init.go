package gotcmd

import (
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
		return got.InitRepo(".")
	},
}
