package gotcmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcns"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/gotbc"
	"go.brendoncarroll.net/star"
)

var blobcacheCmd = star.NewDir(
	star.Metadata{Short: "manage blobcache"},
	map[string]star.Command{
		"create-space": bcCreateSpaceCmd,
	},
)

var bcCreateSpaceCmd = star.Command{
	Metadata: star.Metadata{Short: "create a space volume. Does not modify repo config"},
	Pos:      []star.Positional{volNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		bc := bcclient.NewClientFromEnv()
		bcns, err := bcclient.NewNSClientFromEnv()
		if err != nil {
			return err
		}
		p := volNameParam.Load(c)
		volh, err := bcns.CreateVolume(ctx, p, gotns.SpaceVolumeSpec())
		if err != nil {
			return err
		}
		c.Printf("created volume at %s\n", p)
		u, err := bcsdk.URLFor(ctx, bc, volh)
		if err != nil {
			return err
		}
		var secret [32]byte
		rand.Read(secret[:])
		c.Printf("URL: %v\n", u)
		c.Printf("SECRET: %v\n", hex.EncodeToString(secret[:]))
		return err
	},
}

// createVol create a new volume according to spec at volname in the namespace
func createVol(ctx context.Context, svc blobcache.Service, volName string, spec blobcache.VolumeSpec) (blobcache.Handle, error) {
	root, err := bcclient.EnvNSRoot()
	if err != nil {
		return blobcache.Handle{}, err
	}
	nsc := bcns.NewClient(svc, root)
	return nsc.CreateVolume(ctx, volName, spec)
}

func createRepoVol(ctx context.Context, svc blobcache.Service, volName string) (blobcache.Handle, error) {
	return createVol(ctx, svc, volName, gotrepo.RepoVolumeSpec(false))
}

func createNSVol(ctx context.Context, svc blobcache.Service, volName string) (blobcache.Handle, error) {
	return createVol(ctx, svc, volName, gotns.SpaceVolumeSpec())
}

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
