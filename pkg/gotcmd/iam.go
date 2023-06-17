package gotcmd

import (
	"bufio"
	"fmt"

	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/spf13/cobra"
)

func newIAMCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:      "iam",
		Short:    "Perform Identity and Access Management operations on a repo",
		PreRunE:  loadRepo(&repo, open),
		PostRunE: closeRepo(repo),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := repo.GetHostConfig(ctx)
			if err != nil {
				return err
			}
			w := bufio.NewWriter(cmd.OutOrStdout())
			for _, rule := range cfg.Policy.Rules {
				var allow string
				if rule.Allow {
					allow = "ALLOW"
				} else {
					allow = "DENY"
				}
				fmt.Fprintf(w, "%05s\t%v\t%05s\t%v\n", allow, rule.Subject, rule.Verb, rule.Object)
			}
			return w.Flush()
		},
	}
}
