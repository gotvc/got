package gotcmd

import (
	"os"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/gotvc/got/src/internal/serde"
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
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		p := targetParam.Load(c)
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdOut)
		defer r.Close()
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()

		var root *gotfs.Root
		if err := repo.DoWithStore(ctx, "", func(st stores.RW) error {
			fsag := gotfs.NewMachine()
			var err error
			root, err = fsag.FileFromReader(ctx, [2]stores.RW{st, st}, 0o755, f)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		r.Close()
		pemData, err := serde.MarshalPEM(root)
		if err != nil {
			return err
		}
		c.StdOut.Write(pemData)
		return nil
	},
}

var targetParam = star.Required[string]{
	ID:    "target",
	Parse: star.ParseString,
}
