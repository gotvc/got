package gotcmd

import (
	"github.com/gotvc/got/src/gotrepo"
	"github.com/spf13/cobra"
)

func newServeCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:     "serve",
		Short:   "serves a repository to the network",
		PreRunE: loadRepo(&repo, open),
		RunE: func(cmd *cobra.Command, args []string) error {
			return repo.Serve(ctx)
		},
	}
}

func newServeQUICCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "serve-quic",
		Short:    "serves a repository to the network using QUIC",
		Args:     cobra.ExactArgs(1),
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		RunE: func(cmd *cobra.Command, args []string) error {
			laddr := args[0]
			return repo.ServeQUIC(ctx, laddr)
		},
	}
}
