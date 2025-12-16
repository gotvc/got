package gotcmd

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
)

var branchCmd = star.NewDir(
	star.Metadata{
		Short: "manage branches",
	}, map[string]star.Command{
		"create":   branchCreateCmd,
		"list":     branchListCmd,
		"delete":   branchDeleteCmd,
		"get-head": branchGetRootCmd,
		"inspect":  branchInspectCmd,
		"cp-salt":  branchCpSaltCmd,
	},
)

var branchCreateCmd = star.Command{
	Metadata: star.Metadata{
		Short: "creates a new branch",
	},
	Pos: []star.Positional{branchNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		branchName := branchNameParam.Load(c)
		_, err = repo.CreateBranch(ctx, branchName, branches.NewConfig(false))
		return err
	},
}

var branchListCmd = star.Command{
	Metadata: star.Metadata{
		Short: "lists the branches",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		return repo.ForEachBranch(ctx, func(k string) error {
			fmt.Fprintf(c.StdOut, "%s\n", k)
			return nil
		})
	},
}

var branchDeleteCmd = star.Command{
	Metadata: star.Metadata{
		Short: "deletes a branch",
	},
	Pos: []star.Positional{branchNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		name := branchNameParam.Load(c)
		return repo.DeleteBranch(ctx, name)
	},
}

var branchGetRootCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints the snapshot at the head of the provided branch",
	},
	Pos: []star.Positional{branchNameOptParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		name, _ := branchNameOptParam.LoadOpt(c)
		branchHead, err := repo.GetBranchRoot(ctx, name)
		if err != nil {
			return err
		}
		return prettyPrintJSON(c.StdOut, branchHead)
	},
}

var branchInspectCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints branch metadata",
	},
	Pos: []star.Positional{branchNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		name := branchNameParam.Load(c)
		branch, err := repo.GetBranch(ctx, name)
		if err != nil {
			return err
		}
		return prettyPrintJSON(c.StdOut, branch.Info)
	},
}

var branchCpSaltCmd = star.Command{
	Metadata: star.Metadata{
		Short: "copies the salt from one branch to another",
	},
	Pos: []star.Positional{srcBranchParam, dstBranchParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		src := srcBranchParam.Load(c)
		dst := dstBranchParam.Load(c)
		srcInfo, err := repo.GetBranch(ctx, src)
		if err != nil {
			return err
		}
		dstInfo, err := repo.GetBranch(ctx, dst)
		if err != nil {
			return err
		}
		cfg := dstInfo.AsParams()
		cfg.Salt = srcInfo.Info.Salt
		return repo.SetBranch(ctx, dst, cfg)
	},
}

var branchNameParam = star.Required[string]{
	ID:    "branch_name",
	Parse: star.ParseString,
}

var branchNameOptParam = star.Optional[string]{
	ID:    "branch_name",
	Parse: star.ParseString,
}

var srcBranchParam = star.Required[string]{
	ID:    "src",
	Parse: star.ParseString,
}

var dstBranchParam = star.Required[string]{
	ID:    "dst",
	Parse: star.ParseString,
}

var headCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints or sets the contents of HEAD",
	},
	Pos: []star.Positional{branchNameOptParam},
	F: func(c star.Context) error {
		// Active modifies the working copy not the repo
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		name, hasName := branchNameOptParam.LoadOpt(c)
		if !hasName {
			name, err := wc.GetHead()
			if err != nil {
				return err
			}
			fmt.Fprintln(c.StdOut, name)
			return nil
		}
		return wc.SetHead(c, name)
	},
}

var forkCmd = star.Command{
	Metadata: star.Metadata{
		Short: "creates a new branch based off the provided branch",
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
	Pos: []star.Positional{srcBranchParam, dstBranchParam},
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
		src := srcBranchParam.Load(c)
		dst := dstBranchParam.Load(c)
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
