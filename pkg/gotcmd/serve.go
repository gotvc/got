package gotcmd

import (
	"github.com/brendoncarroll/go-p2p/p/stringmux"
	"github.com/brendoncarroll/got/pkg/gotnet"
	"github.com/inet256/inet256/pkg/inet256p2p"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(serveCmd)
}

var serveCmd = &cobra.Command{
	Use:     "serve",
	Short:   "serves this repository to the network",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		privKey := repo.GetPrivateKey()
		peerswarm, err := inet256p2p.NewSwarm("127.0.0.1:25600", privKey)
		if err != nil {
			return err
		}
		defer peerswarm.Close()
		mux := stringmux.WrapAskSwarm(peerswarm)
		srv := gotnet.New(gotnet.Params{
			ACL: repo.GetACL(),
			Mux: mux,
		})
		return srv.Serve()
	},
}
