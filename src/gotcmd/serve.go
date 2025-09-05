package gotcmd

import (
	"fmt"
	"net"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/spf13/cobra"
)

func newServeCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	return &cobra.Command{
		Use:   "serve <listen-address>",
		Short: "serve the repository",
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, err := open()
			if err != nil {
				return err
			}
			if len(args) < 1 {
				return fmt.Errorf("listen address is required")
			}
			laddrStr := args[0]
			laddr, err := net.ResolveUDPAddr("udp", laddrStr)
			if err != nil {
				return err
			}
			pc, err := net.ListenUDP("udp", laddr)
			if err != nil {
				return err
			}
			leaf, err := repo.ActiveIdentity(ctx)
			if err != nil {
				return err
			}
			ep := blobcache.Endpoint{
				Peer:   leaf.ID,
				IPPort: pc.LocalAddr().(*net.UDPAddr).AddrPort(),
			}
			fmt.Fprintln(cmd.OutOrStdout(), "BLOBCACHE ENDPOINT:")
			fmt.Fprintf(cmd.OutOrStdout(), "%v\n\n", ep)
			defer pc.Close()
			fmt.Fprintln(cmd.OutOrStdout(), "serving...")
			return repo.Serve(ctx, pc)
		},
	}
}
