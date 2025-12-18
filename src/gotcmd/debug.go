package gotcmd

import (
	"github.com/gotvc/got/src/gotrepo"
	"go.brendoncarroll.net/star"
)

var debugCmd = star.Command{
	Metadata: star.Metadata{
		Short: "debug commands",
	},
	Pos: []star.Positional{debugTypeParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		bname, err := wc.GetHead()
		if err != nil {
			return err
		}
		repo := wc.Repo()

		p := debugTypeParam.Load(c)
		switch p {
		case "fs":
			return repo.DebugFS(ctx, gotrepo.FQM{Name: bname}, c.StdOut)
		case "kv":
			return repo.DebugKV(ctx, gotrepo.FQM{Name: bname}, c.StdOut)
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
