package gotcmd

import (
	"github.com/gotvc/got/src/gotrepo"
	"go.brendoncarroll.net/star"
)

var initCmd = star.Command{
	Metadata: star.Metadata{
		Short: "initializes a repository in the current directory",
	},
	F: func(c star.Context) error {
		config := gotrepo.DefaultConfig()
		if err := gotrepo.Init(".", config); err != nil {
			return err
		}
		c.Printf("successfully initialized got repo in current directory\n")
		return nil
	},
}
