package gotcmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
	"golang.org/x/sync/errgroup"
)

var markCmd = star.NewDir(
	star.Metadata{
		Short: "manage the marks in a space and what they point at",
	}, map[string]star.Command{
		"create":  markCreateCmd,
		"list":    markListCmd,
		"del":     markDeleteCmd,
		"inspect": markInspectCmd,
		"as":      markAsCmd,
		"cp":      markCpCmd,
		"mv":      markMvCmd,

		"load":    markLoadCmd,
		"sync":    markSyncCmd,
		"cp-salt": markCpSaltCmd,
	},
)

var markCreateCmd = star.Command{
	Metadata: star.Metadata{
		Short: "creates a new bookmark",
	},
	Flags: map[string]star.Flag{
		"space": spaceNameOptParam,
	},
	Pos: []star.Positional{markNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		branchName := markNameParam.Load(c)
		spaceName, _ := spaceNameOptParam.LoadOpt(c)
		_, err = repo.CreateMark(ctx, gotrepo.FQM{Space: spaceName, Name: branchName}, gotcore.DefaultConfig(false), nil)
		return err
	},
}

var markListCmd = star.Command{
	Metadata: star.Metadata{
		Short: "lists the bookmarks",
	},
	Flags: map[string]star.Flag{
		"space": spaceNameOptParam,
	},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		repo := wc.Repo()
		head, err := wc.GetHead()
		if err != nil {
			return err
		}
		spaceName, _ := spaceNameOptParam.LoadOpt(c)
		hdrs := []any{"NAME", "CREATED_AT", "SALT", "ANNOTATIONS"}
		fmt.Fprintf(c.StdOut, " %-20s %-20s %-8s %-10s\n", hdrs...)
		return repo.ForEachMark(ctx, spaceName, func(k string) error {
			isHead := " "
			if spaceName == "" && k == head {
				isHead = "*"
			}
			info, err := repo.InspectMark(ctx, gotrepo.FQM{Space: spaceName, Name: k})
			if err != nil {
				return err
			}
			createdAt := info.CreatedAt.GoTime().Local().Format(time.DateTime)
			salt := info.Config.Salt[:4]
			annots := len(info.Annotations)
			fmt.Fprintf(c.StdOut, "%s%-20s %-20s %-8x %-10d\n", isHead, k, createdAt, salt, annots)
			return nil
		})
	},
}

var spaceNameOptParam = star.Optional[string]{
	ID:       "space",
	ShortDoc: "the name of the space to access (local space is the default)",
	Parse:    star.ParseString,
}

var markDeleteCmd = star.Command{
	Metadata: star.Metadata{
		Short: "deletes a bookmark",
	},
	Flags: map[string]star.Flag{
		"space": spaceNameOptParam,
	},
	Pos: []star.Positional{markNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		name := markNameParam.Load(c)
		spaceName, _ := spaceNameOptParam.LoadOpt(c)
		return repo.DeleteMark(ctx, gotrepo.FQM{Space: spaceName, Name: name})
	},
}

var markMvCmd = star.Command{
	Metadata: star.Metadata{
		Short: "moves a mark from one name to another within a space",
	},
	Flags: map[string]star.Flag{
		"space": spaceNameOptParam,
	},
	Pos: []star.Positional{srcMarkNameParam, dstMarkNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		space, _ := spaceNameOptParam.LoadOpt(c)
		return repo.MoveMark(ctx, space, srcMarkNameParam.Load(c), dstMarkNameParam.Load(c))
	},
}

var markCpCmd = star.Command{
	Metadata: star.Metadata{
		Short: "copies a mark from one name to another, the new name must be available",
	},
	Pos: []star.Positional{srcMarkParam, dstMarkParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		return repo.CloneMark(ctx, srcMarkParam.Load(c), dstMarkParam.Load(c))
	},
}

var srcMarkNameParam = star.Required[string]{
	ID:    "src-mark",
	Parse: star.ParseString,
}

var dstMarkNameParam = star.Required[string]{
	ID:    "dst-mark",
	Parse: star.ParseString,
}

var markLoadCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints the point that is the target of the mark",
	},
	Flags: map[string]star.Flag{
		"space": spaceNameOptParam,
	},
	Pos: []star.Positional{markNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		space, _ := spaceNameOptParam.LoadOpt(c)
		name := markNameParam.Load(c)
		snap, err := repo.MarkLoad(ctx, gotrepo.FQM{Space: space, Name: name})
		if err != nil {
			return err
		}
		if snap != nil {
			c.Printf("%x\n", snap.Marshal(nil))
		}
		return nil
	},
}

var markInspectCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints any metadata for a bookmark",
	},
	Flags: map[string]star.Flag{
		"space": spaceNameOptParam,
	},
	Pos: []star.Positional{markNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		name := markNameParam.Load(c)
		space, _ := spaceNameOptParam.LoadOpt(c)
		fqm := gotrepo.FQM{Space: space, Name: name}
		return repo.ViewMark(ctx, fqm, func(mt *gotcore.MarkTx) error {
			return prettyPrintJSON(c.StdOut, mt.Info())
		})
	},
}

var markCpSaltCmd = star.Command{
	Metadata: star.Metadata{
		Short: "copies the salt from one bookmark to another",
	},
	Pos: []star.Positional{srcMarkParam, dstMarkParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		src := srcMarkParam.Load(c)
		dst := dstMarkParam.Load(c)
		srcInfo, err := repo.InspectMark(ctx, src)
		if err != nil {
			return err
		}
		dstInfo, err := repo.InspectMark(ctx, dst)
		if err != nil {
			return err
		}
		cfg := dstInfo.Config
		cfg.Salt = srcInfo.Config.Salt
		return repo.ConfigureMark(ctx, dst, gotcore.Metadata{
			Config:      cfg,
			Annotations: dstInfo.Annotations,
		})
	},
}

var markAsCmd = star.Command{
	Metadata: star.Metadata{
		Short: "creates a new bookmark pointed at the target of the current mark",
	},
	Pos: []star.Positional{newMarkNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		head, err := wc.GetHead()
		if err != nil {
			return err
		}
		repo := wc.Repo()
		newfqn := gotrepo.FQM{Name: newMarkNameParam.Load(c)}
		if err := repo.CloneMark(ctx,
			gotrepo.FQM{Name: head},
			newfqn); err != nil {
			return err
		}
		c.Printf("marked snapshot as %v", newfqn.Name)
		return nil
	},
}

var historyCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints the snapshot log",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		repo := wc.Repo()
		bname, err := wc.GetHead()
		if err != nil {
			return err
		}
		pr, pw := io.Pipe()
		eg := errgroup.Group{}
		eg.Go(func() error {
			bufw := bufio.NewWriter(pw)
			err := repo.History(ctx, gotcore.SnapExpr_Mark{Name: bname}, func(ref gdat.Ref, snap gotrepo.Snap) error {
				if err := printSnap(bufw, ref, snap); err != nil {
					return err
				}
				if err := bufw.WriteByte('\n'); err != nil {
					return err
				}
				return bufw.Flush()
			})
			pw.CloseWithError(err)
			return err
		})
		eg.Go(func() error {
			err := pipeToLess(pr)
			// _, err := io.Copy(c.StdOut, pr)
			pr.CloseWithError(err)
			return err
		})
		return eg.Wait()
	},
}

func printSnap(bufw *bufio.Writer, ref gdat.Ref, snap gotcore.Snap) error {
	fmt.Fprintf(bufw, "#%04d\t%v\n", snap.N, ref.CID)
	fmt.Fprintf(bufw, "FS: %v\n", snap.Payload.Root.Ref.CID)
	if len(snap.Parents) == 0 {
		fmt.Fprintf(bufw, "Parents: (none)\n")
	} else {
		fmt.Fprintf(bufw, "Parents:\n")
		for _, parent := range snap.Parents {
			fmt.Fprintf(bufw, "  %v\n", parent.CID)
		}
	}
	fmt.Fprintf(bufw, "Created At: %v\n", snap.CreatedAt.GoTime().Local().String())
	fmt.Fprintf(bufw, "Created By: %v\n", snap.Creator)
	bufw.Write([]byte(prettifyJSON(snap.Payload.Aux)))
	fmt.Fprintln(bufw)
	return nil
}

var markNameParam = star.Required[string]{
	ID:       "mark_name",
	Parse:    star.ParseString,
	ShortDoc: "the name of a mark",
}

var srcMarkParam = star.Required[gotrepo.FQM]{
	ID:       "src",
	Parse:    parseFQName,
	ShortDoc: "the source bookmark",
}

var dstMarkParam = star.Required[gotrepo.FQM]{
	ID:       "dst",
	Parse:    parseFQName,
	ShortDoc: "the destination bookmark",
}

func parseFQName(s string) (gotrepo.FQM, error) {
	return gotrepo.ParseFQName(s), nil
}

var forkCmd = star.Command{
	Metadata: star.Metadata{
		Short: "creates a new mark pointed at the target of the current mark",
	},
	Pos: []star.Positional{newMarkNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		return wc.Fork(ctx, newMarkNameParam.Load(c))
	},
}

var markSyncCmd = star.Command{
	Metadata: star.Metadata{
		Short: "syncs the contents of one bookmark to another",
	},
	Pos: []star.Positional{srcMarkParam, dstMarkParam},
	Flags: map[string]star.Flag{
		"force": forceParam,
	},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdIn, c.StdOut)
		defer r.Close()
		src := srcMarkParam.Load(c)
		dst := dstMarkParam.Load(c)
		force, _ := forceParam.LoadOpt(c)
		return repo.SyncUnit(ctx, src, dst, force)
	},
}

var newMarkNameParam = star.Required[string]{
	ID:       "new_name",
	Parse:    star.ParseString,
	ShortDoc: "the name of the new bookmark",
}

var forceParam = star.Optional[bool]{
	ID: "force",
	Parse: func(s string) (bool, error) {
		if s == "" || s == "true" {
			return true, nil
		}
		return false, nil
	},
}

func prettyPrintJSON(w io.Writer, x any) error {
	data, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.Write(data)
	return err
}

func prettifyJSON(x json.RawMessage) string {
	data, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		return ""
	}
	return string(data)
}
