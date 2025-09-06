package gotcmd

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/gotvc/got/src/branches"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/metrics"
	"github.com/spf13/cobra"
	"go.brendoncarroll.net/tai64"
	"golang.org/x/sync/errgroup"
)

func newCommitCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "commit",
		Short:    "commits the contents of staging applied to the contents of the active volume",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO get message from -m flag
			r := metrics.NewTTYRenderer(collector, cmd.OutOrStdout())
			defer r.Close()
			now := tai64.Now().TAI64()
			return repo.Commit(ctx, branches.SnapInfo{
				Message:    "",
				AuthoredAt: now,
			})
		},
	}
}

func newHistoryCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "history",
		Short:    "prints the snapshot log",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		Aliases:  []string{"log"},
		RunE: func(cmd *cobra.Command, args []string) error {
			pr, pw := io.Pipe()
			eg := errgroup.Group{}
			eg.Go(func() error {
				err := repo.History(ctx, "", func(ref gdat.Ref, c gotvc.Snap) error {
					fmt.Fprintf(pw, "#%04d\t%v\n", c.N, ref.CID)
					fmt.Fprintf(pw, "Created At: %v\n", c.CreatedAt.GoTime().Local().String())
					fmt.Fprintf(pw, "Message: %s\n", c.Aux)
					fmt.Fprintln(pw)
					return nil
				})
				pw.CloseWithError(err)
				return err
			})
			eg.Go(func() error {
				err := pipeToLess(pr)
				//_, err := io.Copy(cmd.OutOrStdout(), pr)
				pr.CloseWithError(err)
				return err
			})
			return eg.Wait()
		},
	}
}

func pipeToLess(r io.Reader) error {
	cmd := exec.Command("/usr/bin/less")
	cmd.Stdin = r
	cmd.Stdout = os.Stdout
	return cmd.Run()
}
