package gotcmd

import (
	"bufio"
	"fmt"

	"github.com/spf13/cobra"
)

var iamCmd = &cobra.Command{
	Use:     "iam",
	Short:   "Perform Identity and Access Management operations on a repo",
	PreRunE: loadRepo,
	RunE: func(cmd *cobra.Command, args []string) error {
		pol := repo.GetIAMPolicy()
		w := bufio.NewWriter(cmd.OutOrStdout())
		for _, rule := range pol.Rules {
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
