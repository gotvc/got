package gotcmd

import (
	"encoding/json"

	"go.brendoncarroll.net/star"
)

var nsCmd = star.NewDir(star.Metadata{
	Short: "manage namespaces",
}, map[string]star.Command{
	"ls": nsListCmd,
})

var nsListCmd = star.Command{
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
		return nil
	},
}
