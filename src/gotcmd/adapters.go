package gotcmd

import (
	"net"
	"net/http"

	"github.com/spf13/cobra"
	"go.brendoncarroll.net/stdctx/logctx"
	ftpserver "goftp.io/server/v2"

	"github.com/gotvc/got/src/adapters/gotftp"
	"github.com/gotvc/got/src/adapters/gotiofs"
	"github.com/gotvc/got/src/gotrepo"
)

func newHTTPCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	c := &cobra.Command{
		Use:   "http [branch]",
		Short: "serve files over HTTP",
	}
	laddr := c.Flags().String("addr", "127.0.0.1:6006", "--addr 127.0.0.1:12345")
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
		fs := gotiofs.New(ctx, *b)
		h := http.FileServer(http.FS(fs))
		l, err := net.Listen("tcp", *laddr)
		if err != nil {
			return err
		}
		defer l.Close()
		logctx.Infof(ctx, "serving on http://%v", l.Addr())
		return http.Serve(l, h)
	}
	return c
}

func newFTPCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	c := &cobra.Command{
		Use:   "ftp [branch]",
		Short: "serve files over FTP",
	}
	laddr := c.Flags().String("addr", "127.0.0.1:6006", "--addr 127.0.0.1:12345")
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
	}
	return c
}

type ftpAuth struct {
}

func (a ftpAuth) CheckPasswd(ctx *ftpserver.Context, user string, param string) (bool, error) {
	return true, nil
}
