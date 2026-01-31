package gotcmd

import (
	"bufio"
	"fmt"

	"github.com/fatih/color"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/marks"
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
		if _, err := fmt.Fprintf(bufw, "HEAD: %s\n", name); err != nil {
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
			_, err := fmt.Fprintf(bufw, "  %7s %s\n", desc, p)
			return err
		}); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(bufw, "DIRTY:\n"); err != nil {
			return err
		}
		if err := wc.ForEachDirty(ctx, func(fi gotwc.DirtyFile) error {
			if fi.Exists {
				fmt.Fprintf(bufw, "  + %v %s\n", fi.Mode, fi.Path)
			} else {
				fmt.Fprintf(bufw, "  - %s\n", fi.Path)
			}
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
	Flags: map[string]star.Flag{
		"snap": snapExprOptParam,
	},
	Pos: []star.Positional{pathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		se, ok := snapExprOptParam.LoadOpt(c)
		if !ok {
			mname, err := wc.GetHead()
			if err != nil {
				return err
			}
			se = &marks.SnapExpr_Mark{Name: mname}
		}
		p, _ := pathParam.LoadOpt(c)
		return wc.Repo().Ls(ctx, se, p, func(ent gotfs.DirEnt) error {
			_, err := fmt.Fprintf(c.StdOut, "%v %s\n", ent.Mode, ent.Name)
			return err
		})
	},
}

var catCmd = star.Command{
	Metadata: star.Metadata{
		Short: "writes the contents of path in the current volume to stdout",
	},
	Flags: map[string]star.Flag{
		"snap": snapExprOptParam,
	},
	Pos: []star.Positional{pathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		se, ok := snapExprOptParam.LoadOpt(c)
		if !ok {
			mname, err := wc.GetHead()
			if err != nil {
				return err
			}
			se = &marks.SnapExpr_Mark{Name: mname}
		}
		p, _ := pathParam.LoadOpt(c)
		return wc.Repo().Cat(ctx, se, p, c.StdOut)
	},
}

var snapExprOptParam = star.Optional[gotrepo.SnapExpr]{
	ID:       "snap",
	Parse:    marks.ParseSnapExpr,
	ShortDoc: "a snapshot expression",
}

var fqmnOptParam = star.Optional[gotrepo.FQM]{
	ID: "mark-fq",
	Parse: func(s string) (gotrepo.FQM, error) {
		return gotrepo.ParseFQName(s), nil
	},
	ShortDoc: "a fully qualified mark name",
}

var pathParam = star.Optional[string]{
	ID:    "path",
	Parse: star.ParseString,
}
