package gotcmd

import (
	"fmt"
	"os"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/serde"
	"github.com/gotvc/got/src/internal/stores"
	"github.com/spf13/cobra"
)

func newSlurpCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "slurp",
		Short:    "imports a file or directory and returns a ref",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("must provide target to ingest")
			}
			r := metrics.NewTTYRenderer(collector, cmd.OutOrStdout())
			defer r.Close()
			p := args[0]
			f, err := os.Open(p)
			if err != nil {
				return err
			}
			defer f.Close()

			var root *gotfs.Root
			if err := repo.DoWithStore(ctx, "", func(st stores.RW) error {
				fsag := gotfs.NewMachine()
				var err error
				root, err = fsag.FileFromReader(ctx, [2]stores.RW{st, st}, 0o755, f)
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			r.Close()
			pemData, err := serde.MarshalPEM(root)
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			w.Write(pemData)
			return nil
		},
	}
}
