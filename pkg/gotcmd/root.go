package gotcmd

import (
	"github.com/gotvc/got"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(initCmd)

	rootCmd.AddCommand(commitCmd)
	rootCmd.AddCommand(historyCmd)
	rootCmd.AddCommand(addCmd)
	rootCmd.AddCommand(rmCmd)
	rootCmd.AddCommand(putCmd)
	rootCmd.AddCommand(discardCmd)
	rootCmd.AddCommand(clearCmd)

	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(lsCmd)
	rootCmd.AddCommand(catCmd)

	rootCmd.AddCommand(branchCmd)
	rootCmd.AddCommand(activeCmd)
	rootCmd.AddCommand(forkCmd)

	rootCmd.AddCommand(iamCmd)
	rootCmd.AddCommand(localIDCmd)
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(serveQUICCmd)

	rootCmd.AddCommand(slurpCmd)
	rootCmd.AddCommand(cleanupCmd)
	rootCmd.AddCommand(debugCmd)
	rootCmd.AddCommand(derefCmd)
	rootCmd.AddCommand(scrubCmd)
}

var rootCmd = &cobra.Command{
	Use:   "got",
	Short: "got is like git, but with an 'o'",
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
