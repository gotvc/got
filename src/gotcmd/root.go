package gotcmd

import (
	"context"

	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"

	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/metrics"
)

// Main is the main function for the got CLI.
func Main() {
	logger := func() *zap.Logger {
		log, _ := zap.NewProduction()
		return log
	}()
	collector := metrics.NewCollector()

	ctx := context.Background()
	ctx = logctx.NewContext(ctx, logger)
	ctx = metrics.WithCollector(ctx, collector)
	star.Main(rootCmd, star.MainBackground(ctx))
}

// Root returns the root command for the got CLI.
func Root() star.Command {
	return rootCmd
}

var rootCmd = star.NewDir(
	star.Metadata{
		Short: "got is like git, but with an 'o'",
	}, map[string]star.Command{
		"init": initCmd,

		"status": statusCmd,

		// staging area commands
		"add":     addCmd,
		"rm":      rmCmd,
		"put":     putCmd,
		"discard": discardCmd,
		"clear":   clearCmd,
		"commit":  commitCmd,

		// other working copy methods
		"head": headCmd,
		"wc":   wcCmd,

		"ls":   lsCmd,
		"cat":  catCmd,
		"http": httpCmd,
		"ftp":  ftpCmd,

		"history": historyCmd,
		"log":     historyCmd,
		"mark":    mark,
		"sync":    syncCmd,
		"fork":    forkCmd,

		"ns": nsCmd,

		"serve": serveCmd,
		"slurp": slurpCmd,
		"debug": debugCmd,
		"scrub": scrubCmd,
		"bc":    blobcacheCmd,
	},
)

func openRepo() (*gotrepo.Repo, error) {
	return gotrepo.Open(".")
}

func openWC() (*gotwc.WC, error) {
	return gotwc.Open(".")
}
