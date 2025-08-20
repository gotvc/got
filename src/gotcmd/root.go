package gotcmd

import (
	"context"

	"github.com/spf13/cobra"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"

	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/metrics"
)

func Execute() error {
	return NewRootCmd().Execute()
}

func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "got",
		Short: "got is like git, but with an 'o'",
	}
	open := func() (*gotrepo.Repo, error) {
		return gotrepo.Open(".")
	}
	for _, cmd := range []*cobra.Command{
		newInitCmd(),
	} {
		rootCmd.AddCommand(cmd)
	}
	for _, cmdf := range []func(func() (*gotrepo.Repo, error)) *cobra.Command{
		newCommitCmd,
		newHistoryCmd,
		newAddCmd,
		newRmCmd,
		newPutCmd,
		newDiscardCmd,
		newClearCmd,

		newStatusCmd,
		newLsCmd,
		newCatCmd,
		newBranchCmd,
		newActiveCmd,
		newForkCmd,
		newSyncCmd,

		newLocalIDCmd,
		newHTTPCmd,
		newFTPCmd,

		newSlurpCmd,
		newCleanupCmd,
		newDebugCmd,
		newScrubCmd,
	} {
		rootCmd.AddCommand(cmdf(open))
	}
	return rootCmd
}

var (
	log = func() *zap.Logger {
		log, _ := zap.NewProduction()
		return log
	}()
	collector = metrics.NewCollector()

	ctx = func() context.Context {
		ctx := context.Background()
		ctx = logctx.NewContext(ctx, log)
		ctx = metrics.WithCollector(ctx, collector)
		return ctx
	}()
)

func loadRepo(repo **gotrepo.Repo, open func() (*gotrepo.Repo, error)) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		var err error
		*repo, err = open()
		return err
	}
}

func closeRepo(repo *gotrepo.Repo) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if repo == nil {
			return nil
		}
		return repo.Close()
	}
}
