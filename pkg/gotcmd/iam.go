package gotcmd

import (
	"bufio"
	"fmt"

	"github.com/brendoncarroll/go-exp/slices2"
	"github.com/gotvc/got/pkg/branches"
	"github.com/gotvc/got/pkg/gothost"
	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func newIAMCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	c := &cobra.Command{
		Use:                "iam",
		Short:              "Perform Identity and Access Management operations on a repo",
		PersistentPreRunE:  loadRepo(&repo, open),
		PersistentPostRunE: closeRepo(repo),
	}
	branchPrefix := c.PersistentFlags().StringP("prefix", "p", "", "-p origin/")
	modify := func(fn func(gothost.State) (*gothost.State, error)) error {
		space := repo.GetSpace()
		if *branchPrefix != "" {
			space = branches.NewPrefixSpace(space, *branchPrefix)
		}
		e := gothost.NewHostEngine(space)
		return e.Modify(ctx, fn)
	}
	showCmd := &cobra.Command{
		Use:   "show",
		Short: "prints identities and access policy",
		RunE: func(cmd *cobra.Command, args []string) error {
			space := repo.GetSpace()
			if *branchPrefix != "" {
				space = branches.NewPrefixSpace(space, *branchPrefix)
			}
			e := gothost.NewHostEngine(space)
			cfg, err := e.View(ctx)
			if err != nil {
				return err
			}
			w := bufio.NewWriter(cmd.OutOrStdout())
			fmt.Fprintln(w, "IDENTITIES")
			fmt.Fprintln(w, "NAME\tOWNERS\tMEMBERS")
			names := maps.Keys(cfg.Identities)
			slices.Sort(names)
			for _, name := range names {
				iden := cfg.Identities[name]
				fmt.Fprintf(w, "%s\t%v\t%v\n", name, iden.Owners, iden.Members)
			}
			fmt.Fprintln(w)
			fmt.Fprintln(w, "POLICY")
			for i, rule := range cfg.Policy.Rules {
				fmt.Fprintf(w, "%d) ", i)
				if _, err := w.Write(gothost.MarshalRule(rule)); err != nil {
					return err
				}
			}
			return w.Flush()
		},
	}
	createIden := &cobra.Command{
		Use:   "create-id",
		Short: "creates a new identity",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			return modify(func(x gothost.State) (*gothost.State, error) {
				if _, exists := x.Identities[name]; exists {
					return nil, fmt.Errorf("an identity with name %q already exists", name)
				}
				x.Identities[name] = gothost.Identity{
					Owners: []gothost.IdentityElement{gothost.NewNamed(name)},
				}
				return &x, nil
			})
		},
	}
	deleteIden := &cobra.Command{
		Use:   "del-id",
		Short: "deletes an identity",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			return modify(func(x gothost.State) (*gothost.State, error) {
				delete(x.Identities, name)
				return &x, nil
			})
		},
	}
	addMember := &cobra.Command{
		Use:   "add-mem",
		Short: "adds a member to an identity",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			idenElems, err := parseIdenElems(args[1:])
			if err != nil {
				return err
			}
			return modify(func(x gothost.State) (*gothost.State, error) {
				if _, exists := x.Identities[name]; !exists {
					return nil, fmt.Errorf("identity %q does not exist", name)
				}
				iden := x.Identities[name]
				iden.Members = append(x.Identities[name].Members, idenElems...)
				x.Identities[name] = iden
				return &x, nil
			})
		},
	}
	rmMember := &cobra.Command{
		Use:   "rm-mem",
		Short: "removes a member from an identity",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			idenElems, err := parseIdenElems(args[1:])
			if err != nil {
				return err
			}
			return modify(func(x gothost.State) (*gothost.State, error) {
				if _, exists := x.Identities[name]; !exists {
					return nil, fmt.Errorf("identity %q does not exist", name)
				}
				iden := x.Identities[name]
				iden.Members = slices2.Filter(iden.Members, func(ie gothost.IdentityElement) bool {
					return slices.Contains(idenElems, ie)
				})
				x.Identities[name] = iden
				return &x, nil
			})
		},
	}
	rmOwner := &cobra.Command{
		Use:   "rm-own",
		Short: "removes an owner from an identity",
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			idenElems, err := parseIdenElems(args[1:])
			if err != nil {
				return err
			}
			return modify(func(x gothost.State) (*gothost.State, error) {
				if _, exists := x.Identities[name]; !exists {
					return nil, fmt.Errorf("identity %q does not exist", name)
				}
				iden := x.Identities[name]
				iden.Owners = slices2.Filter(iden.Owners, func(ie gothost.IdentityElement) bool {
					return slices.Contains(idenElems, ie)
				})
				x.Identities[name] = iden
				return &x, nil
			})
		},
	}
	c.AddCommand(showCmd)
	c.AddCommand(createIden)
	c.AddCommand(deleteIden)
	c.AddCommand(addMember)
	c.AddCommand(rmMember)
	c.AddCommand(rmOwner)
	return c
}

func parseIdenElems(xs []string) (ret []gothost.IdentityElement, _ error) {
	for _, ieStr := range xs {
		ie, err := gothost.ParseIDElement([]byte(ieStr))
		if err != nil {
			return nil, err
		}
		ret = append(ret, ie)
	}
	return ret, nil
}
