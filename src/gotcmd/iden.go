package gotcmd

import (
	"go.brendoncarroll.net/star"
)

var idenCmd = star.NewDir(star.Metadata{
	Short: "manage identities",
}, map[string]star.Command{
	"ls": idenListCmd,
})

var idenListCmd = star.Command{
	Metadata: star.Metadata{
		Short: "list the identities available in the repo",
	},
	F: func(c star.Context) error {
		return nil
	},
}
