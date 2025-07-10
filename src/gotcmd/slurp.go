package gotcmd

import (
	"fmt"
	"os"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/serde"
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
			p := args[0]
			f, err := os.Open(p)
			if err != nil {
				return err
			}
			defer f.Close()

			st, err := repo.GetImportStores(ctx, "")
			if err != nil {
				return err
			}
			fsag := gotfs.NewMachine()
			root, err := fsag.FileFromReader(ctx, st.FS, st.Raw, 0o755, f)
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
