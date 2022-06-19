package gotcmd

import (
	"fmt"

	"github.com/gotvc/got/pkg/gotrepo"
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
			id := repo.GetID()
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "%v\n", id)
			return nil
		},
	}
}
