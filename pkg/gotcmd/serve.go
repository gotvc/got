package gotcmd

import (
	"github.com/brendoncarroll/got/pkg/gotnet"
	"github.com/brendoncarroll/got/pkg/p2pkv"
	"github.com/inet256/inet256/pkg/inet256p2p"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(serveCmd)
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "serves this repository to the network",
	RunE: func(cmd *cobra.Command, args []string) error {
		privKey := repo.GetPrivateKey()
		peerswarm, err := inet256p2p.NewSwarm("127.0.0.1:25600", privKey)
		if err != nil {
			return err
		}
		srv := gotnet.NewServer(repo)
		return p2pkv.Serve(ctx, peerswarm, srv)
	},
}
