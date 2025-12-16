package gotcmd

import (
	"bufio"
	"fmt"

	"github.com/fatih/color"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotwc"
	"go.brendoncarroll.net/star"
)

var statusCmd = star.Command{
	Metadata: star.Metadata{
		Short: "shows the status of the working tree",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		name, err := wc.GetHead()
		if err != nil {
			return err
		}
		bufw := bufio.NewWriter(c.StdOut)
		if _, err := fmt.Fprintf(bufw, "BRANCH: %s\n", name); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(bufw, "STAGED:\n"); err != nil {
			return err
		}
		if err := wc.ForEachStaging(ctx, func(p string, op gotwc.FileOperation) error {
			var desc = "UNKNOWN"
			switch {
			case op.Delete != nil:
				desc = color.RedString("DELETE")
			case op.Create != nil:
				desc = color.BlueString("CREATE")
			case op.Modify != nil:
				desc = color.GreenString("MODIFY")
			}
			_, err := fmt.Fprintf(bufw, "\t%7s %s\n", desc, p)
			return err
		}); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(bufw, "UNTRACKED:\n"); err != nil {
			return err
		}
		if err := wc.ForEachUntracked(ctx, func(p string) error {
			_, err := fmt.Fprintf(bufw, "\t%s\n", p)
			return err
		}); err != nil {
			return err
		}
		return bufw.Flush()
	},
}

var lsCmd = star.Command{
	Metadata: star.Metadata{
		Short: "lists the children of path in the current volume",
	},
	Pos: []star.Positional{pathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		b, err := wc.GetHead()
		if err != nil {
			return err
		}
		p, _ := pathParam.LoadOpt(c)
		return wc.Repo().Ls(ctx, b, p, func(ent gotfs.DirEnt) error {
			_, err := fmt.Fprintf(c.StdOut, "%v %s\n", ent.Mode, ent.Name)
			return err
		})
	},
}

var catCmd = star.Command{
	Metadata: star.Metadata{
		Short: "writes the contents of path in the current volume to stdout",
	},
	Pos: []star.Positional{pathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		b, err := wc.GetHead()
		if err != nil {
			return err
		}
		p, _ := pathParam.LoadOpt(c)
		return wc.Repo().Cat(ctx, b, p, c.StdOut)
	},
}

var scrubCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs integrity checks on the current branch",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		b, err := wc.GetHead()
		if err != nil {
			return err
		}
		return wc.Repo().Check(ctx, b)
	},
}

var pathParam = star.Optional[string]{
	ID:    "path",
	Parse: star.ParseString,
}
