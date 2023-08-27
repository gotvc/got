package gotcmd

import (
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/spf13/cobra"
)

func newDebugCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "debug",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		Hidden:   true,
		Args:     cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			p := args[0]
			switch p {
			case "db":
				return repo.DebugDB(ctx, cmd.OutOrStdout())
			case "fs":
				return repo.DebugFS(ctx, cmd.OutOrStdout())
			case "kv":
				return repo.DebugKV(ctx, cmd.OutOrStdout())
			default:
				return nil
			}
		},
	}
}

func newDerefCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "deref",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		Hidden:   true,
		Args:     cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			p := args[0]
			var ref gdat.Ref
			if err := ref.UnmarshalText([]byte(p)); err != nil {
				return err
			}
			s := repo.UnionStore()
			dag := gdat.NewAgent()
			return dag.GetF(ctx, s, ref, func(data []byte) error {
				_, err := cmd.OutOrStdout().Write(data)
				return err
			})
		},
	}

}
