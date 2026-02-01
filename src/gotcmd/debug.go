package gotcmd

import (
	"github.com/gotvc/got/src/internal/gotcore"
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
		se := &gotcore.SnapExpr_Mark{Space: "", Name: bname}
		switch p {
		case "fs":
			return repo.DebugFS(ctx, se, c.StdOut)
		case "kv":
			return repo.DebugKV(ctx, se, c.StdOut)
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
