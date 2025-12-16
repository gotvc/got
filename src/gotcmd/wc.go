package gotcmd

import (
	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
)

var wcCmd = star.NewDir(star.Metadata{
	Short: "commands for managing working copies",
},
	map[string]star.Command{
		"cleanup": cleanupCmd,
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
