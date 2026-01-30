package gotcmd

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotrepo"
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
		"mark": markFQNParam,
	},
	Pos: []star.Positional{pathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		fqm, ok := markFQNParam.LoadOpt(c)
		if !ok {
			mname, err := wc.GetHead()
			if err != nil {
				return err
			}
			fqm.Space = ""
			fqm.Name = mname
		}
		p, _ := pathParam.LoadOpt(c)
		return wc.Repo().Ls(ctx, fqm, p, func(ent gotfs.DirEnt) error {
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
		"mark": markFQNParam,
	},
	Pos: []star.Positional{pathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		fqm, ok := markFQNParam.LoadOpt(c)
		if !ok {
			mname, err := wc.GetHead()
			if err != nil {
				return err
			}
			fqm.Space = ""
			fqm.Name = mname
		}
		p, _ := pathParam.LoadOpt(c)
		return wc.Repo().Cat(ctx, fqm, p, c.StdOut)
	},
}

var markFQNParam = star.Optional[gotrepo.FQM]{
	ID: "mark-fq",
	Parse: func(s string) (gotrepo.FQM, error) {
		parts := strings.SplitN(s, ":", 2)
		if len(parts) == 1 {
			return gotrepo.FQM{Name: parts[0]}, nil
		}
		return gotrepo.FQM{Space: parts[0], Name: parts[1]}, nil
	},
}

var pathParam = star.Optional[string]{
	ID:    "path",
	Parse: star.ParseString,
}
