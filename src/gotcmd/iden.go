package gotcmd

import (
	"maps"
	"slices"

	"go.brendoncarroll.net/star"
)

var idenCmd = star.NewDir(star.Metadata{
	Short: "manage identities",
}, map[string]star.Command{
	"list": idenListCmd,
})

var idenListCmd = star.Command{
	Metadata: star.Metadata{
		Short: "list the identities available in the repo",
	},
	F: func(c star.Context) error {
		repo, err := openRepo()
		if err != nil {
			return err
		}
		idens, err := repo.Identities(c)
		if err != nil {
			return err
		}
		keys := slices.Collect(maps.Keys(idens))
		slices.Sort(keys)
		c.Printf("%-16s %-64s\n", "NAME", "ID")
		for _, name := range keys {
			idu := idens[name]
			c.Printf("%-16s %-64v\n", name, idu.ID)
		}
		return nil
	},
}
