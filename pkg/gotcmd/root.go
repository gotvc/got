package gotcmd

import (
	"github.com/gotvc/got"
	"github.com/gotvc/got/pkg/branches"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

func Execute() error {
	return NewRootCmd().Execute()
}

func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "got",
		Short: "got is like git, but with an 'o'",
	}
	for _, cmd := range []*cobra.Command{
		initCmd,
		newCloneCmd(),

		commitCmd,
		historyCmd,
		addCmd,
		rmCmd,
		putCmd,
		discardCmd,
		clearCmd,

		statusCmd,
		lsCmd,
		catCmd,
		branchCmd,
		activeCmd,
		forkCmd,

		iamCmd,
		localIDCmd,
		serveCmd,
		serveQUICCmd,
		newHTTPUICmd(getSpace),

		slurpCmd,
		cleanupCmd,
		debugCmd,
		derefCmd,
		scrubCmd,
	} {
		rootCmd.AddCommand(cmd)
	}
	return rootCmd
}

var (
	repo *got.Repo
	ctx  = context.Background()
)

func loadRepo(cmd *cobra.Command, args []string) error {
	r, err := got.OpenRepo(".")
	if err != nil {
		return err
	}
	repo = r
	return nil
}

func closeRepo(cmd *cobra.Command, args []string) error {
	return repo.Close()
}

func getSpace() branches.Space {
	return repo.GetSpace()
}
