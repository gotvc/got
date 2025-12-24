package gotcmd

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/tai64"
)

var addCmd = star.Command{
	Metadata: star.Metadata{
		Short: "adds paths to the staging area",
	},
	Pos: []star.Positional{pathsParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		paths := pathsParam.Load(c)
		if len(paths) < 1 {
			return fmt.Errorf("path argument required")
		}
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdOut)
		defer r.Close()
		return wc.Add(ctx, paths...)
	},
}

var rmCmd = star.Command{
	Metadata: star.Metadata{
		Short: "stages paths for deletion",
	},
	Pos: []star.Positional{pathsParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		paths := pathsParam.Load(c)
		if len(paths) < 1 {
			return fmt.Errorf("path argument required")
		}
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdOut)
		defer r.Close()
		return wc.Rm(ctx, paths...)
	},
}

var putCmd = star.Command{
	Metadata: star.Metadata{
		Short: "stages paths for replacement",
	},
	Pos: []star.Positional{pathsParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		paths := pathsParam.Load(c)
		if len(paths) < 1 {
			return fmt.Errorf("path argument required")
		}
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdOut)
		defer r.Close()
		return wc.Put(ctx, paths...)
	},
}

var discardCmd = star.Command{
	Metadata: star.Metadata{
		Short: "discards any changes staged for a path",
	},
	Pos: []star.Positional{pathsParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		paths := pathsParam.Load(c)
		if len(paths) < 1 {
			return fmt.Errorf("path argument required")
		}
		return wc.Discard(ctx, paths...)
	},
}

var clearCmd = star.Command{
	Metadata: star.Metadata{
		Short: "clears the staging area",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		return wc.Clear(ctx)
	},
}

var commitCmd = star.Command{
	Metadata: star.Metadata{
		Short: "commits the contents of staging applied to the contents of the active volume",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		// TODO get message from -m flag
		r := metrics.NewTTYRenderer(metrics.FromContext(ctx), c.StdOut)
		defer r.Close()
		now := tai64.Now().TAI64()
		return wc.Commit(ctx, gotwc.CommitParams{
			Message:    "",
			AuthoredAt: now,
		})
	},
}

var pathsParam = star.Repeated[string]{
	ID:       "paths",
	ShortDoc: "one or more paths to add, remove, put, or discard",
	Parse:    star.ParseString,
}

func pipeToLess(r io.Reader) error {
	cmd := exec.Command("/usr/bin/less")
	cmd.Stdin = r
	cmd.Stdout = os.Stdout
	return cmd.Run()
}
