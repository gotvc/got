package gotcmd

import (
	"fmt"
	"os"

	"github.com/gotvc/got"
	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/gotvc/got/pkg/goturl"
	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	return &cobra.Command{
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
}

func newCloneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clone <dst> <url>",
		Short: "clone creates a new repository at dst using url for a space",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dst, u := args[0], args[1]
			url, err := goturl.ParseSpaceURL(u)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(dst, 0o755); err != nil {
				return err
			}
			return gotrepo.Clone(ctx, *url, dst)
		},
	}
}
