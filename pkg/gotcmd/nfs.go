package gotcmd

import (
	"fmt"
	"net"
	"net/netip"

	"github.com/gotvc/got/pkg/gotbilly"
	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/gotvc/got/pkg/logctx"
	"github.com/spf13/cobra"
	"github.com/willscott/go-nfs"
	nfshelpers "github.com/willscott/go-nfs/helpers"
)

func newNFSCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	c := &cobra.Command{
		Use:   "nfs [branch]",
		Short: "serves files over NFS",
	}
	laddr := c.Flags().String("addr", "127.0.0.1:6049", "--addr 127.0.0.1:12345")
	c.RunE = func(cmd *cobra.Command, args []string) error {
		var branchName string
		if len(args) > 0 {
			branchName = args[0]
		}
		repo, err := open()
		if err != nil {
			return err
		}
		b, err := repo.GetBranch(ctx, branchName)
		if err != nil {
			return err
		}
		l, err := net.Listen("tcp", *laddr)
		if err != nil {
			return err
		}
		defer l.Close()
		addrport, err := netip.ParseAddrPort(l.Addr().String())
		if err != nil {
			return err
		}
		fsx := gotbilly.New(ctx, b)
		handler := nfshelpers.NewNullAuthHandler(fsx)
		handler = nfshelpers.NewCachingHandler(handler, 1)
		logctx.Infof(ctx, "serving NFS on %v ...", l.Addr())
		logctx.Infof(ctx, "mount with:\n\t%s\n\t%s", "mkdir /private/nfs", mountCmd(addrport.Port()))
		return nfs.Serve(l, handler)
	}
	return c
}

func mountCmd(port uint16) string {
	return fmt.Sprintf("mount -o port=%d,mountport=%d -t nfs localhost:/mount /private/nfs", port, port)
}
