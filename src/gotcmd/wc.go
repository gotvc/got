package gotcmd

import (
	"fmt"

	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
)

var wcCmd = star.NewDir(star.Metadata{
	Short: "commands for managing working copies",
},
	map[string]star.Command{
		"cleanup": cleanupCmd,
		"add":     addCmd,
		"rm":      rmCmd,
		"discard": discardCmd,
		"clear":   clearCmd,
		"head":    headCmd,
	},
)

var cleanupCmd = star.Command{
	Metadata: star.Metadata{
		Short: "cleanup cleans up unreferenced data in the staging area",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		ctx, cf := metrics.Child(ctx, "cleanup")
		defer cf()
		return wc.Cleanup(ctx)
	},
}

var headCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints or sets the contents of HEAD",
	},
	Pos: []star.Positional{markNameOptParam},
	F: func(c star.Context) error {
		// Active modifies the working copy not the repo
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		name, hasName := markNameOptParam.LoadOpt(c)
		if !hasName {
			name, err := wc.GetHead()
			if err != nil {
				return err
			}
			fmt.Fprintln(c.StdOut, name)
			return nil
		}
		return wc.SetHead(c, name)
	},
}
