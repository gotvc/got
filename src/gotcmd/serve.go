package gotcmd

import (
	"fmt"
	"net"
	"os"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/gotbc"
	"go.brendoncarroll.net/star"
)

var serveCmd = star.Command{
	Metadata: star.Metadata{
		Short: "serves the local space over Blobcache Protocol (BCP)",
	},
	Pos: []star.Positional{listenAddrParam},
	F: func(c star.Context) error {
		ctx := c.Context
		root, err := os.OpenRoot(".")
		if err != nil {
			return err
		}
		cfg, err := gotwc.LoadConfig(root)
		if err != nil {
			return err
		}
		if cfg.Blobcache.InProcess == nil {
			return fmt.Errorf("serve requires a local blobcache")
		}
		bcSvc, err := gotbc.OpenBlobcache(root, cfg.Blobcache, c)
		if err != nil {
			return err
		}
		bc := bcSvc.(*bclocal.Service)

		laddrStr := listenAddrParam.Load(c)
		laddr, err := net.ResolveUDPAddr("udp", laddrStr)
		if err != nil {
			return err
		}
		pc, err := net.ListenUDP("udp", laddr)
		if err != nil {
			return err
		}
		ep := blobcache.Endpoint{
			Node:   bc.LocalID(),
			IPPort: pc.LocalAddr().(*net.UDPAddr).AddrPort(),
		}
		fmt.Fprintln(c.StdOut, "BLOBCACHE ENDPOINT:")
		fmt.Fprintf(c.StdOut, "%v\n\n", ep)
		defer pc.Close()
		fmt.Fprintln(c.StdOut, "serving...")
		return bc.Serve(ctx, pc)
	},
}

var listenAddrParam = &star.Required[string]{
	PosName: "listen-address",
	Parse:   star.ParseString,
}
