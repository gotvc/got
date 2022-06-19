package gotcmd

import (
	"context"
	"os"

	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/gotvc/got/pkg/serde"
	"github.com/pkg/errors"
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
				return errors.Errorf("must provide target to ingest")
			}
			p := args[0]
			f, err := os.Open(p)
			if err != nil {
				return err
			}
			defer f.Close()

			st, err := repo.GetImportStores(context.Background(), "")
			if err != nil {
				return err
			}
			fsop := gotfs.NewOperator()
			root, err := fsop.CreateFileRoot(ctx, st.FS, st.Raw, f)
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			data, err := serde.MarshalPEM(root)
			if err != nil {
				return err
			}
			w.Write(data)
			return nil
		},
	}

}
