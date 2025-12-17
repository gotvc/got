package gotcmd

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
)

var mark = star.NewDir(
	star.Metadata{
		Short: "manage the contents of a namespace",
	}, map[string]star.Command{
		"create":   markCreateCmd,
		"list":     markListCmd,
		"delete":   markDeleteCmd,
		"get-root": markGetTargetCmd,
		"inspect":  markInspectCmd,
		"cp-salt":  markCpSaltCmd,
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
		_, err = repo.CreateMark(ctx, branchName, branches.NewConfig(false))
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
		return repo.ForEachMark(ctx, "", func(k string) error {
			fmt.Fprintf(c.StdOut, "%s\n", k)
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
		return repo.DeleteMark(ctx, name)
	},
}

var markGetTargetCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints the snapshot that is the target of the mark",
	},
	Pos: []star.Positional{markNameOptParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		name, _ := markNameOptParam.LoadOpt(c)
		branchHead, err := repo.GetMarkRoot(ctx, name)
		if err != nil {
			return err
		}
		return prettyPrintJSON(c.StdOut, branchHead)
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
		branch, err := repo.GetMark(ctx, name)
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
		srcInfo, err := repo.GetMark(ctx, src)
		if err != nil {
			return err
		}
		dstInfo, err := repo.GetMark(ctx, dst)
		if err != nil {
			return err
		}
		cfg := dstInfo.AsParams()
		cfg.Salt = srcInfo.Info.Salt
		return repo.ConfigureMark(ctx, dst, cfg)
	},
}

var markNameParam = star.Required[string]{
	ID:    "mark_name",
	Parse: star.ParseString,
}

var markNameOptParam = star.Optional[string]{
	ID:    "mark_name",
	Parse: star.ParseString,
}

var srcMarkParam = star.Required[string]{
	ID:    "src",
	Parse: star.ParseString,
}

var dstMarkParam = star.Required[string]{
	ID:    "dst",
	Parse: star.ParseString,
}

var forkCmd = star.Command{
	Metadata: star.Metadata{
		Short: "creates a new mark based off the provided branch",
	},
	Pos: []star.Positional{newBranchNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		newName := newBranchNameParam.Load(c)
		rend := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdOut)
		defer rend.Close()
		current, err := wc.GetHead()
		if err != nil {
			return err
		}
		if err := wc.Repo().Fork(ctx, current, newName); err != nil {
			return err
		}
		return wc.SetHead(ctx, newName)
	},
}

var syncCmd = star.Command{
	Metadata: star.Metadata{
		Short: "syncs the contents of one branch to another",
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
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdOut)
		defer r.Close()
		src := srcMarkParam.Load(c)
		dst := dstMarkParam.Load(c)
		force, _ := forceParam.LoadOpt(c)
		return repo.Sync(ctx, src, dst, force)
	},
}

var newBranchNameParam = star.Required[string]{
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
