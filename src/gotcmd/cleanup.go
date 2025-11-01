package gotcmd

import (
	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
)

var cleanupCmd = star.Command{
	Metadata: star.Metadata{
		Short: "cleanup cleans up unreferenced data associated with branches",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		ctx, cf := metrics.Child(ctx, "cleanup")
		defer cf()
		return repo.Cleanup(ctx)
	},
}
