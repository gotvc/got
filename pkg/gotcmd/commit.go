package gotcmd

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/brendoncarroll/got"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func init() {
	rootCmd.AddCommand(commitCmd)
	rootCmd.AddCommand(logCmd)
}

var commitCmd = &cobra.Command{
	Use:     "commit",
	Short:   "commits the contents of staging applied to the contents of the active volume",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO get message from -m flag
		now := time.Now()
		return repo.Commit(ctx, "", &now)
	},
}

var logCmd = &cobra.Command{
	Use:     "log",
	Short:   "prints the log",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		pr, pw := io.Pipe()
		eg := errgroup.Group{}
		eg.Go(func() error {
			err := repo.Log(ctx, func(ref got.Ref, c got.Commit) error {
				fmt.Fprintf(pw, "#%04d\t%v\n", c.N, ref.CID)
				fmt.Fprintf(pw, "Created At: %v\n", c.CreatedAt)
				fmt.Fprintf(pw, "Message: %s\n", c.Message)
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

func pipeToLess(r io.Reader) error {
	cmd := exec.Command("/usr/bin/less")
	cmd.Stdin = r
	cmd.Stdout = os.Stdout
	return cmd.Run()
}