package gotcmd

import (
	"net"
	"net/http"

	"github.com/gotvc/got/pkg/gotiofs"
	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/gotvc/got/pkg/logctx"
	"github.com/spf13/cobra"
)

func newHTTPUICmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	c := &cobra.Command{
		Use:   "httpui [branch]",
		Short: "serve a UI over HTTP",
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
		fs := gotiofs.New(b)
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
