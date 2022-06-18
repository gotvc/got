package gotcmd

import (
	"net"
	"net/http"

	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gotiofs"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func newHTTPUICmd(sf func() branches.Space) *cobra.Command {
	c := &cobra.Command{
		Use:     "httpui <branch>",
		Short:   "serve a UI over HTTP",
		Args:    cobra.MinimumNArgs(1),
		PreRunE: loadRepo,
	}
	laddr := c.Flags().String("addr", "127.0.0.1:6006", "--addr 127.0.0.1:12345")
	c.RunE = func(cmd *cobra.Command, args []string) error {
		branchName := args[0]
		space := getSpace()
		fs := gotiofs.New(space, branchName)
		h := http.FileServer(http.FS(fs))
		l, err := net.Listen("tcp", *laddr)
		if err != nil {
			return err
		}
		defer l.Close()
		logrus.Infof("serving on http://%v", l.Addr())
		return http.Serve(l, h)
	}
	return c
}
