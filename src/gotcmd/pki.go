package gotcmd

import (
	"fmt"

	"github.com/gotvc/got/src/gotrepo"
	"github.com/spf13/cobra"
)

func newLocalIDCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "local-id",
		Short:    "prints the local ID",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		RunE: func(cmd *cobra.Command, args []string) error {
			leaf, err := repo.ActiveIdentity(ctx)
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "%-40s\t%-10s\t%-10s\n", "ID", "SIG_ALGO", "KEM_ALGO")
			fmt.Fprintf(w, "%-40v\t%-10s\t%-10s\n", leaf.ID, leaf.PublicKey.Scheme().Name(), leaf.KEMPublicKey.Scheme().Name())
			return nil
		},
	}
}
