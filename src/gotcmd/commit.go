package gotcmd

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/metrics"
	"go.brendoncarroll.net/star"
	"go.brendoncarroll.net/tai64"
	"golang.org/x/sync/errgroup"
)

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
		return wc.Commit(ctx, branches.SnapInfo{
			Message:    "",
			AuthoredAt: now,
		})
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
			err := repo.History(ctx, bname, func(ref gdat.Ref, snap gotvc.Snap) error {
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

func pipeToLess(r io.Reader) error {
	cmd := exec.Command("/usr/bin/less")
	cmd.Stdin = r
	cmd.Stdout = os.Stdout
	return cmd.Run()
}
