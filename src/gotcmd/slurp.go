package gotcmd

import (
	"encoding/json"
	"os"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/star"
)

var slurpCmd = star.Command{
	Metadata: star.Metadata{
		Short: "imports a file or directory and returns a ref",
	},
	Pos: []star.Positional{targetParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		p := targetParam.Load(c)
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdIn, c.StdOut)
		defer r.Close()
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()

		var exts []gotfs.Extent
		if err := wc.DoWithStore(ctx, func(st stores.RW) error {
			fsmach := gotfs.NewMachine(gotfs.Params{})
			var err error
			exts, err = fsmach.ExtentsFromReader(ctx, gotfs.RW{Metadata: st, Data: st}, f)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		r.Close()
		pemData, err := json.MarshalIndent(exts, "", "  ")
		if err != nil {
			return err
		}
		c.StdOut.Write(pemData)
		return nil
	},
}

var targetParam = &star.Required[string]{
	PosName: "target",
	Parse:   star.ParseString,
}
