package gotcmd

import (
	"encoding/json"

	"github.com/gotvc/got/src/gotrepo"
	"go.brendoncarroll.net/star"
)

var spaceCmd = star.NewDir(star.Metadata{
	Short: "manage namespaces",
}, map[string]star.Command{
	"ls":   spaceListCmd,
	"sync": spaceSyncCmd,
})

var spaceListCmd = star.Command{
	Metadata: star.Metadata{Short: "list namespaces"},
	F: func(c star.Context) error {
		repo, err := openRepo()
		if err != nil {
			return err
		}
		spaces, err := repo.ListSpaces(c)
		if err != nil {
			return err
		}
		for _, scfg := range spaces {
			data, _ := json.Marshal(scfg.Spec)
			c.Printf("%s %s\n", scfg.Name, data)
		}
		if len(spaces) == 0 {
			c.Printf("  (no spaces other than the default space)\n")
		}
		return nil
	},
}

var spaceSyncCmd = star.Command{
	Metadata: star.Metadata{
		Short: "copies marks from one space to another",
	},
	Pos: []star.Positional{srcSpaceParam, dstSpaceParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		task := gotrepo.SyncSpacesTask{
			Src: srcSpaceParam.Load(c),
			Dst: dstSpaceParam.Load(c),
		}
		return repo.SyncSpaces(ctx, task)
	},
}

var srcSpaceParam = star.Required[string]{
	ID:    "src_space",
	Parse: star.ParseString,
}

var dstSpaceParam = star.Required[string]{
	ID:    "dst_space",
	Parse: star.ParseString,
}
