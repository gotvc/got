package gotcmd

import (
	"fmt"

	"go.brendoncarroll.net/star"
)

var localIDCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints the local ID",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		leaf, err := repo.ActiveIdentity(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintf(c.StdOut, "%-40s\t%-10s\t%-10s\n", "ID", "SIG_ALGO", "KEM_ALGO")
		fmt.Fprintf(c.StdOut, "%-40v\t%-10s\t%-10s\n", leaf.ID, leaf.PublicKey.Scheme().Name(), leaf.KEMPublicKey.Scheme().Name())
		return nil
	},
}
