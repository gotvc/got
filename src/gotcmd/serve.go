package gotcmd

import (
	"fmt"
	"net"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/star"
)

var serveCmd = star.Command{
	Metadata: star.Metadata{
		Short: "serves the local space over Blobcache Protocol (BCP)",
	},
	Pos: []star.Positional{listenAddrParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		if cfg := repo.Config(); cfg.Blobcache.InProcess == nil {
			return fmt.Errorf("serve requires a repository with a local blobcache")
		}
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
			Peer:   repo.BlobcachePeer(),
			IPPort: pc.LocalAddr().(*net.UDPAddr).AddrPort(),
		}
		fmt.Fprintln(c.StdOut, "BLOBCACHE ENDPOINT:")
		fmt.Fprintf(c.StdOut, "%v\n\n", ep)
		defer pc.Close()
		fmt.Fprintln(c.StdOut, "serving...")
		return repo.Serve(ctx, pc)
	},
}

var listenAddrParam = star.Required[string]{
	ID:    "listen-address",
	Parse: star.ParseString,
}
