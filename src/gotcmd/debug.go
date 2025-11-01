package gotcmd

import (
	"go.brendoncarroll.net/star"
)

var debugCmd = star.Command{
	Metadata: star.Metadata{
		Short: "debug commands",
	},
	Pos: []star.Positional{debugTypeParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		p := debugTypeParam.Load(c)
		switch p {
		case "fs":
			return repo.DebugFS(ctx, c.StdOut)
		case "kv":
			return repo.DebugKV(ctx, c.StdOut)
		default:
			return nil
		}
	},
}

var debugTypeParam = star.Required[string]{
	ID:       "type",
	ShortDoc: "the type of debug to run",
	Parse:    star.ParseString,
}
