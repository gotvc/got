package gotcmd

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/marks"
	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
	"golang.org/x/sync/errgroup"
)

var mark = star.NewDir(
	star.Metadata{
		Short: "manage the marks in a space and what they point at",
	}, map[string]star.Command{
		"create":  markCreateCmd,
		"list":    markListCmd,
		"delete":  markDeleteCmd,
		"load":    markLoadCmd,
		"inspect": markInspectCmd,
		"cp-salt": markCpSaltCmd,
		"sync":    markSyncCmd,
		"as":      markAsCmd,
	},
)

var markCreateCmd = star.Command{
	Metadata: star.Metadata{
		Short: "creates a new mark",
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
		_, err = repo.CreateMark(ctx, gotrepo.FQM{Name: branchName}, marks.DefaultConfig(false), nil)
		return err
	},
}

var markListCmd = star.Command{
	Metadata: star.Metadata{
		Short: "lists the marks",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		fmt.Fprintf(c.StdOut, "%-20s %-20s %-10s\n", "NAME", "CREATED_AT", "ANNOTATIONS")
		return repo.ForEachMark(ctx, "", func(k string) error {
			info, err := repo.InspectMark(ctx, gotrepo.FQM{Name: k})
			if err != nil {
				return err
			}
			createdAt := info.CreatedAt.GoTime().Local().Format(time.DateTime)
			annots := len(info.Annotations)
			fmt.Fprintf(c.StdOut, "%-20s %-20s %-10d\n", k, createdAt, annots)
			return nil
		})
	},
}

var markDeleteCmd = star.Command{
	Metadata: star.Metadata{
		Short: "deletes a mark",
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
		return repo.DeleteMark(ctx, gotrepo.FQM{Name: name})
	},
}

var markLoadCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints the snapshot that is the target of the mark",
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
		snap, err := repo.MarkLoad(ctx, gotrepo.FQM{Name: name})
		if err != nil {
			return err
		}
		c.Printf("%x\n", snap.Marshal(nil))
		return nil
	},
}

var markInspectCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints any metadata for a mark",
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
		branch, err := repo.GetMark(ctx, gotrepo.FQM{Name: name})
		if err != nil {
			return err
		}
		return prettyPrintJSON(c.StdOut, branch.Info)
	},
}

var markCpSaltCmd = star.Command{
	Metadata: star.Metadata{
		Short: "copies the salt from one mark to another",
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
		return repo.ConfigureMark(ctx, dst, marks.Metadata{
			Config:      cfg,
			Annotations: dstInfo.Annotations,
		})
	},
}

var markAsCmd = star.Command{
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
			err := repo.History(ctx, gotrepo.FQM{Name: bname}, func(ref gdat.Ref, snap gotrepo.Snap) error {
				fmt.Fprintf(pw, "#%04d\t%v\n", snap.N, ref.CID)
				fmt.Fprintf(pw, "FS: %v\n", snap.Payload.Root.Ref.CID)
				if len(snap.Parents) == 0 {
					fmt.Fprintf(pw, "Parents: (none)\n")
				} else {
					fmt.Fprintf(pw, "Parents:\n")
					for _, parent := range snap.Parents {
						fmt.Fprintf(pw, "  %v\n", parent.CID)
					}
				}
				fmt.Fprintf(pw, "Created At: %v\n", snap.CreatedAt.GoTime().Local().String())
				fmt.Fprintf(pw, "Created By: %v\n", snap.Creator)
				pw.Write([]byte(prettifyJSON(snap.Payload.Aux)))
				fmt.Fprintln(pw)
				fmt.Fprintln(pw)
				return nil
			})
			pw.CloseWithError(err)
			return err
		})
		eg.Go(func() error {
			err := pipeToLess(pr)
			//_, err := io.Copy(c.Stdout, pr)
			pr.CloseWithError(err)
			return err
		})
		return eg.Wait()
	},
}

var markNameParam = star.Required[string]{
	ID:    "mark_name",
	Parse: star.ParseString,
}

var srcMarkParam = star.Required[gotrepo.FQM]{
	ID:    "src",
	Parse: parseFQName,
}

var dstMarkParam = star.Required[gotrepo.FQM]{
	ID:    "dst",
	Parse: parseFQName,
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
		Short: "syncs the contents of one mark to another",
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
	ID:    "new_name",
	Parse: star.ParseString,
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
