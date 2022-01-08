package gotcmd

import (
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:     "serve",
	Short:   "serves a repository to the network",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		return repo.Serve(ctx)
	},
}

var serveQUICCmd = &cobra.Command{
	Use:     "serve-quic",
	Short:   "serves a repository to the network using QUIC",
	Args:    cobra.ExactArgs(1),
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		laddr := args[0]
		return repo.ServeQUIC(ctx, laddr)
	},
}
