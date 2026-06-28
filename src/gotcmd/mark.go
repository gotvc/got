package gotcmd

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/gotcore"
	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
)

var markCmd = star.NewDir(
	star.Metadata{
		Short: "manage the marks in a space and what they point at",
	}, map[string]star.Command{
		"create":  markCreateCmd,
		"list":    markListCmd,
		"del":     markDeleteCmd,
		"delp":    markDeletePrefixCmd,
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
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
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
		head, err := wc.GetSaveTo()
		if err != nil {
			return err
		}
		spaceName, _ := spaceNameOptParam.LoadOpt(c)
		hdrs := []any{"NAME", "CREATED_AT", "SALT", "TARGET", "ANNOTATIONS"}
		fmt.Fprintf(c.StdOut, " %-20s %-20s %-8s %-8s %-10s\n", hdrs...)
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
			target := "(empty)"
			ref, err := repo.MarkLoad(ctx, gotrepo.FQM{Space: spaceName, Name: k})
			if err != nil {
				return err
			}
			if !ref.IsZero() {
				target = ref.CID.String()[:8]
			}
			annots := len(info.Annotations)
			fmt.Fprintf(c.StdOut, "%s%-20s %-20s %-8x %-8s %-10d\n", isHead, k, createdAt, salt, target, annots)
			return nil
		})
	},
}

var spaceNameOptParam = &star.Optional[string]{
	PosName:  "space",
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
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		name := markNameParam.Load(c)
		spaceName, _ := spaceNameOptParam.LoadOpt(c)
		return repo.DeleteMark(ctx, gotrepo.FQM{Space: spaceName, Name: name})
	},
}

var markDeletePrefixCmd = star.Command{
	Metadata: star.Metadata{
		Short: "deletes all bookmarks with a name prefix",
	},
	Flags: map[string]star.Flag{
		"space": spaceNameOptParam,
	},
	Pos: []star.Positional{markNamePrefixParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		prefix := markNamePrefixParam.Load(c)
		spaceName, _ := spaceNameOptParam.LoadOpt(c)
		toDelete := []string{}
		if err := repo.ForEachMark(ctx, spaceName, func(name string) error {
			if strings.HasPrefix(name, prefix) {
				toDelete = append(toDelete, name)
			}
			return nil
		}); err != nil {
			return err
		}
		for _, name := range toDelete {
			if err := repo.DeleteMark(ctx, gotrepo.FQM{Space: spaceName, Name: name}); err != nil {
				return err
			}
		}
		return nil
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
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
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
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		return repo.CloneMark(ctx, srcMarkParam.Load(c), dstMarkParam.Load(c))
	},
}

var srcMarkNameParam = &star.Required[string]{
	PosName: "src-mark",
	Parse:   star.ParseString,
}

var dstMarkNameParam = &star.Required[string]{
	PosName: "dst-mark",
	Parse:   star.ParseString,
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
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		space, _ := spaceNameOptParam.LoadOpt(c)
		name := markNameParam.Load(c)
		ref, comm, err := repo.MarkLoadCommit(ctx, gotrepo.FQM{Space: space, Name: name})
		if err != nil {
			return err
		}
		if !ref.IsZero() {
			c.Printf("%x\n", comm.Marshal(nil))
		}
		return nil
	},
}

var markInspectCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints any metadata for a bookmark",
	},
	Pos: []star.Positional{fqmParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		fqm := fqmParam.Load(c)
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
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		src := srcMarkParam.Load(c)
		dst := dstMarkParam.Load(c)
		srcInfo, err := repo.InspectMark(ctx, src)
		if err != nil {
			return err
		}
		if err := repo.ConfigureMark(ctx, dst, func(m gotcore.Metadata) gotcore.Metadata {
			m.Config.Salt = srcInfo.Config.Salt
			return m
		}); err != nil {
			return err
		}
		c.Printf("OK: configured %v: salt=%x", dst, srcInfo.Config.Salt[:])
		return nil
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
		head, err := wc.GetSaveTo()
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
		c.Printf("marked commit as %v", newfqn.Name)
		return nil
	},
}

var fqmParam = &star.Required[gotrepo.FQM]{
	PosName: "fqm",
	Parse: func(s string) (gotrepo.FQM, error) {
		return gotrepo.ParseFQName(s), nil
	},
	ShortDoc: "fully qualified mark name",
}

var markNameParam = &star.Required[string]{
	PosName:  "mark_name",
	Parse:    star.ParseString,
	ShortDoc: "the name of a mark",
}

var markNamePrefixParam = &star.Required[string]{
	PosName:  "name-prefix",
	Parse:    star.ParseString,
	ShortDoc: "prefix for mark names",
}

var srcMarkParam = &star.Required[gotrepo.FQM]{
	PosName:  "src-mark",
	Parse:    parseFQName,
	ShortDoc: "the source bookmark",
}

var dstMarkParam = &star.Required[gotrepo.FQM]{
	PosName:  "dst-mark",
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
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdIn, c.StdOut)
		defer r.Close()
		src := srcMarkParam.Load(c)
		dst := dstMarkParam.Load(c)
		force, _ := forceParam.LoadOpt(c)
		return repo.SyncUnit(ctx, src, dst, force)
	},
}

var newMarkNameParam = &star.Required[string]{
	PosName:  "new_name",
	Parse:    star.ParseString,
	ShortDoc: "the name of the new bookmark",
}

var forceParam = &star.Optional[bool]{
	PosName: "force",
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

var mergeCmd = star.Command{
	Metadata: star.Metadata{
		Short: "merges other bookmarks into the current bookmark",
	},
	Pos: []star.Positional{mergeNamesParam},
	F: func(c star.Context) error {
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		return wc.BeginMerge(c, mergeNamesParam.Load(c))
	},
}

var mergeNamesParam = &star.Repeated[string]{
	ShortDoc: "list of marks to merge",
	PosName:  "mark-names",
	Parse:    star.ParseString,
}
