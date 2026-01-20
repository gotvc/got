package gotcmd

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"

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

var rootCmd = star.NewGroupedDir(
	star.Metadata{
		Short: "got is like git, but with an 'o'",
	}, []star.Group{
		{Title: "REPO", Commands: []string{
			"init",
			"serve",
			"scrub",
			"iden",
		}},
		{Title: "WORKING COPY & STAGING", Commands: []string{
			"wc",
			"status",
			"add",
			"rm",
			"put",
			"discard",
			"clear",
			"commit",
			"head",
			"fork",
			"checkout",
		}},
		{Title: "MARKS", Commands: []string{
			"mark",
			"history",
			"cat",
			"ls",
		}},
		{Title: "SPACES", Commands: []string{
			"space",
			"push",
			"fetch",
		}},
		{Title: "ADAPTERS", Commands: []string{
			"http",
			"ftp",
		}},
		{Title: "MISCELLANEOUS", Commands: []string{
			"bc",
			"version",
		}},
	}, map[string]star.Command{
		"init":   initCmd,
		"status": statusCmd,

		// staging area commands
		"add":     addCmd,
		"rm":      rmCmd,
		"put":     putCmd,
		"discard": discardCmd,
		"clear":   clearCmd,
		"commit":  commitCmd,

		// other working copy methods
		"wc":       wcCmd,
		"head":     headCmd,
		"fork":     forkCmd,
		"checkout": checkoutCmd,

		"ls":   lsCmd,
		"cat":  catCmd,
		"http": httpCmd,
		"ftp":  ftpCmd,

		// marks
		"mark":    mark,
		"history": historyCmd,
		"log":     historyCmd,

		"space": spaceCmd,
		"fetch": fetchCmd,
		"push":  pushCmd,

		// misc
		"iden":    idenCmd,
		"org":     orgCmd,
		"serve":   serveCmd,
		"slurp":   slurpCmd,
		"debug":   debugCmd, // intentionally left out of the groups above.
		"scrub":   scrubCmd,
		"bc":      blobcacheCmd,
		"version": versionCmd,
	},
)

func openRepo() (*gotrepo.Repo, error) {
	r, err := os.OpenRoot(".")
	if err != nil {
		return nil, err
	}
	return gotrepo.Open(r)
}

func openWC() (*gotwc.WC, error) {
	r, err := os.OpenRoot(".")
	if err != nil {
		return nil, err
	}
	return gotwc.Open(r)
}

var versionCmd = star.Command{
	Metadata: star.Metadata{Short: "prints version information"},
	F: func(c star.Context) error {
		binfo, ok := debug.ReadBuildInfo()
		if !ok {
			return fmt.Errorf("no build info")
		}
		c.Printf("GO VERSION:  %s\n", binfo.GoVersion)
		c.Printf("GOT VERSION: %s\n", binfo.Main.Version)
		for _, bs := range binfo.Settings {
			switch bs.Key {
			case "vcs.revision", "vcs.time", "vcs.modified":
				c.Printf("%s: %s\n", bs.Key, bs.Value)
			}
		}
		return nil
	},
}
