package gotcmd

import (
	"net"
	"net/http"

	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/stdctx/logctx"
	ftpserver "goftp.io/server/v2"

	"github.com/gotvc/got/src/adapters/gotftp"
	"github.com/gotvc/got/src/adapters/gotiofs"
)

var httpCmd = star.Command{
	Metadata: star.Metadata{
		Short: "serve files over HTTP",
	},
	Pos: []star.Positional{branchNameOptParam},
	Flags: map[string]star.Flag{
		"addr": addrParam,
	},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		branchName, _ := branchNameOptParam.LoadOpt(c)
		b, err := repo.GetBranch(ctx, branchName)
		if err != nil {
			return err
		}
		fs := gotiofs.New(ctx, *b)
		h := http.FileServer(http.FS(fs))
		addr, _ := addrParam.LoadOpt(c)
		if addr == "" {
			addr = "127.0.0.1:6006"
		}
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		defer l.Close()
		logctx.Infof(ctx, "serving on http://%v", l.Addr())
		return http.Serve(l, h)
	},
}

var ftpCmd = star.Command{
	Metadata: star.Metadata{
		Short: "serve files over FTP",
	},
	Pos: []star.Positional{branchNameOptParam},
	Flags: map[string]star.Flag{
		"addr": addrParam,
	},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		branchName, _ := branchNameOptParam.LoadOpt(c)
		b, err := repo.GetBranch(ctx, branchName)
		if err != nil {
			return err
		}
		addr, _ := addrParam.LoadOpt(c)
		if addr == "" {
			addr = "127.0.0.1:6006"
		}
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		defer l.Close()
		s, err := ftpserver.NewServer(&ftpserver.Options{
			Auth:   ftpAuth{},
			Driver: gotftp.NewDriver(ctx, *b),
			Perm:   ftpserver.NewSimplePerm("owner", "group"),
		})
		if err != nil {
			return err
		}
		logctx.Infof(ctx, "serving on ftp://%v", l.Addr())
		return s.Serve(l)
	},
}

var addrParam = star.Optional[string]{
	ID:       "addr",
	ShortDoc: "the address to serve on",
	Parse:    star.ParseString,
}

type ftpAuth struct {
}

func (a ftpAuth) CheckPasswd(ctx *ftpserver.Context, user string, param string) (bool, error) {
	return true, nil
}
