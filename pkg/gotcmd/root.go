package gotcmd

import (
	"github.com/gotvc/got"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

func Execute() error {
	return rootCmd.Execute()
}

var rootCmd = &cobra.Command{
	Use:     "got",
	Short:   "got is like git, but with an 'o'",
	Version: "0.0.0",
}

var (
	repo *got.Repo
	ctx  = context.Background()
)

func loadRepo(cmd *cobra.Command, args []string) error {
	r, err := got.OpenRepo(".")
	if err != nil {
		return err
	}
	repo = r
	return nil
}

func closeRepo(cmd *cobra.Command, args []string) error {
	return repo.Close()
}
