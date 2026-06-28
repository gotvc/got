package gotcmd

import (
	"bufio"
	"fmt"

	"github.com/fatih/color"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
)

var wcCmd = star.NewDir(star.Metadata{
	Short: "commands for managing working copies",
},
	map[string]star.Command{
		"cleanup":  cleanupCmd,
		"add":      addCmd,
		"rm":       rmCmd,
		"discard":  discardCmd,
		"clear":    clearCmd,
		"head":     headCmd,
		"export":   exportCmd,
		"clobber":  clobberCmd,
		"checkout": checkoutCmd,
	},
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
		name, err := wc.GetSaveTo()
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

var cleanupCmd = star.Command{
	Metadata: star.Metadata{
		Short: "cleanup cleans up unreferenced data in the staging area",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		ctx, cf := metrics.Child(ctx, "cleanup")
		defer cf()
		return wc.Cleanup(ctx)
	},
}

var headCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints or sets HEAD.  HEAD is the name of a mark in the local Space",
	},
	Pos: []star.Positional{markNameOptParam},
	F: func(c star.Context) error {
		// Active modifies the working copy not the repo
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		name, hasName := markNameOptParam.LoadOpt(c)
		if !hasName {
			name, err := wc.GetSaveTo()
			if err != nil {
				return err
			}
			fmt.Fprintln(c.StdOut, name)
			return nil
		}
		return wc.SetHead(c, name)
	},
}

var exportCmd = star.Command{
	Metadata: star.Metadata{
		Short: "exports files from the repo to the working copy",
	},
	Pos: []star.Positional{},
	F: func(c star.Context) error {
		// Active modifies the working copy not the repo
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		return wc.Export(c.Context)
	},
}

var clobberCmd = star.Command{
	Metadata: star.Metadata{
		Short: "overwrites files without any checks",
	},
	Pos: []star.Positional{pathParam},
	F: func(c star.Context) error {
		// Active modifies the working copy not the repo
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		p, _ := pathParam.LoadOpt(c)
		return wc.Clobber(c.Context, p)
	},
}

var checkoutCmd = star.Command{
	Metadata: star.Metadata{
		Short: "switches HEAD to the specified mark and then performs an export",
	},
	Pos: []star.Positional{localMarkNameParam},
	F: func(c star.Context) error {
		// Active modifies the working copy not the repo
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		return wc.Checkout(c.Context, localMarkNameParam.Load(c))
	},
}

var localMarkNameParam = &star.Required[string]{
	PosName: "mark_name",
	Parse:   star.ParseString,
}

var markNameOptParam = &star.Optional[string]{
	PosName: "mark_name",
	Parse:   star.ParseString,
}
