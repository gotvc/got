package gotcmd

import (
	"go.brendoncarroll.net/star"
)

var orgCmd = star.NewDir(star.Metadata{
	Short: "create and manage organizations",
}, map[string]star.Command{})
