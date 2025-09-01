package gotcmd

import (
	"github.com/gotvc/got/src/gotrepo"
	"github.com/spf13/cobra"
)

func newDebugCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "debug",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		Hidden:   true,
		Args:     cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			p := args[0]
			switch p {
			case "fs":
				return repo.DebugFS(ctx, cmd.OutOrStdout())
			case "kv":
				return repo.DebugKV(ctx, cmd.OutOrStdout())
			default:
				return nil
			}
		},
	}
}
