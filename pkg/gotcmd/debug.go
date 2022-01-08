package gotcmd

import (
	"github.com/gotvc/got/pkg/gdat"
	"github.com/spf13/cobra"
)

var debugCmd = &cobra.Command{
	Use:     "debug",
	PreRunE: loadRepo,
	Hidden:  true,
	Args:    cobra.ExactArgs(1),
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

var derefCmd = &cobra.Command{
	Use:     "deref",
	PreRunE: loadRepo,
	Hidden:  true,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		p := args[0]
		var ref gdat.Ref
		if err := ref.UnmarshalText([]byte(p)); err != nil {
			return err
		}
		s := repo.UnionStore()
		dop := gdat.NewOperator()
		return dop.GetF(ctx, s, ref, func(data []byte) error {
			_, err := cmd.OutOrStdout().Write(data)
			return err
		})
	},
}
