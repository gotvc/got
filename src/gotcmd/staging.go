package gotcmd

import (
	"fmt"

	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
)

var addCmd = star.Command{
	Metadata: star.Metadata{
		Short: "adds paths to the staging area",
	},
	Pos: []star.Positional{pathsParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		paths := pathsParam.Load(c)
		if len(paths) < 1 {
			return fmt.Errorf("path argument required")
		}
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdOut)
		defer r.Close()
		return repo.Add(ctx, paths...)
	},
}

var rmCmd = star.Command{
	Metadata: star.Metadata{
		Short: "stages paths for deletion",
	},
	Pos: []star.Positional{pathsParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		paths := pathsParam.Load(c)
		if len(paths) < 1 {
			return fmt.Errorf("path argument required")
		}
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdOut)
		defer r.Close()
		return repo.Rm(ctx, paths...)
	},
}

var putCmd = star.Command{
	Metadata: star.Metadata{
		Short: "stages paths for replacement",
	},
	Pos: []star.Positional{pathsParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		paths := pathsParam.Load(c)
		if len(paths) < 1 {
			return fmt.Errorf("path argument required")
		}
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdOut)
		defer r.Close()
		return repo.Put(ctx, paths...)
	},
}

var discardCmd = star.Command{
	Metadata: star.Metadata{
		Short: "discards any changes staged for a path",
	},
	Pos: []star.Positional{pathsParam},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		paths := pathsParam.Load(c)
		if len(paths) < 1 {
			return fmt.Errorf("path argument required")
		}
		return repo.Discard(ctx, paths...)
	},
}

var clearCmd = star.Command{
	Metadata: star.Metadata{
		Short: "clears the staging area",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		return repo.Clear(ctx)
	},
}

var pathsParam = star.Repeated[string]{
	ID:       "paths",
	ShortDoc: "one or more paths to add, remove, put, or discard",
	Parse:    star.ParseString,
}
