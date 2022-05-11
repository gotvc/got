package gotcmd

import (
	"os"

	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/gotvc/got/pkg/goturl"
	"github.com/spf13/cobra"
)

func newCloneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clone <dst> <url>",
		Short: "clone creates a new repository at dst using url for a space",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dst, u := args[0], args[1]
			url, err := goturl.ParseURL(u)
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
