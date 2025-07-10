// Package gothost provides tools for configuring access permissions on the host.
package gothost

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"regexp"

	"github.com/gotvc/got/src/branches/branchintc"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/graphs"
	"go.brendoncarroll.net/exp/slices2"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/posixfs"
	"go.inet256.org/inet256/pkg/inet256"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

const (
	// HostConfigKey is the branch key for the host config
	HostConfigKey = "__host__"

	IdentitiesPath = "IDENTITIES"
	RolesPath      = "ROLES"
	PolicyPath     = "POLICY"
)

type PeerID = inet256.Addr

type State struct {
	Policy     Policy
	Identities map[string]Identity
	Roles      map[string]Role
}

func (s *State) Load(ctx context.Context, fsag *gotfs.Machine, ms, ds cadata.Store, root gotfs.Root) error {
	// identities
	maps.Clear(s.Identities)
	if s.Identities == nil {
		s.Identities = make(map[string]Identity)
	}
	if err := fsag.ReadDir(ctx, ms, root, IdentitiesPath, func(e gotfs.DirEnt) error {
		data, err := fsag.ReadFile(ctx, ms, ds, root, path.Join(IdentitiesPath, e.Name), 1<<16)
		if err != nil {
			return err
		}
		var iden Identity
		if err := json.Unmarshal(data, &iden); err != nil {
			return err
		}
		s.Identities[e.Name] = iden
		return nil
	}); err != nil && !posixfs.IsErrNotExist(err) {
		return err
	}

	// policy
	polData, err := fsag.ReadFile(ctx, ms, ds, root, PolicyPath, 1<<20)
	if err != nil && !posixfs.IsErrNotExist(err) {
		return err
	} else if err == nil {
		pol, err := ParsePolicy(polData)
		if err != nil {
			return err
		}
		s.Policy = *pol
	}

	// roles
	if s.Roles == nil {
		s.Roles = make(map[string]Role)
	}
	if err := fsag.ReadDir(ctx, ms, root, RolesPath, func(e gotfs.DirEnt) error {
		data, err := fsag.ReadFile(ctx, ms, ds, root, path.Join(RolesPath, e.Name), 1<<16)
		if err != nil {
			return err
		}
		var role Role
		if err := json.Unmarshal(data, &role); err != nil {
			return err
		}
		s.Roles[e.Name] = role
		return nil
	}); err != nil && !posixfs.IsErrNotExist(err) {
		return err
	}
	return nil
}

func (s *State) Save(ctx context.Context, fsag *gotfs.Machine, ms, ds cadata.Store) (*gotfs.Root, error) {
	b := fsag.NewBuilder(ctx, ms, ds)
	if err := b.Mkdir("", 0o755); err != nil {
		return nil, err
	}

	// identities
	if err := b.Mkdir(IdentitiesPath, 0o755); err != nil {
		return nil, err
	}
	identityKeys := maps.Keys(s.Identities)
	slices.Sort(identityKeys)
	for _, k := range identityKeys {
		iden := s.Identities[k]
		data, err := json.Marshal(iden)
		if err != nil {
			return nil, err
		}
		if err := b.BeginFile(path.Join(IdentitiesPath, k), 0o644); err != nil {
			return nil, err
		}
		if _, err := b.Write(data); err != nil {
			return nil, err
		}
	}

	// Policy
	if err := b.BeginFile(PolicyPath, 0o644); err != nil {
		return nil, err
	}
	if _, err := b.Write(MarshalPolicy(s.Policy)); err != nil {
		return nil, err
	}

	// Roles
	if err := b.Mkdir(RolesPath, 0o755); err != nil {
		return nil, err
	}
	roleKeys := maps.Keys(s.Roles)
	slices.Sort(roleKeys)
	for _, k := range roleKeys {
		role := s.Roles[k]
		data, err := json.Marshal(role)
		if err != nil {
			return nil, err
		}
		if err := b.BeginFile(path.Join(RolesPath, k), 0o644); err != nil {
			return nil, err
		}
		if _, err := b.Write(data); err != nil {
			return nil, err
		}
	}

	return b.Finish()
}

func (s *State) Validate() error {
	cycle := graphs.FindCycle(maps.Keys(s.Identities), func(out []string, v string) []string {
		for _, elem := range s.Identities[v].Union {
			if elem.Name != nil {
				out = append(out, *elem.Name)
			}
		}
		return out
	})
	if len(cycle) != 0 {
		return fmt.Errorf("identities contain cycle %v", cycle)
	}
	return nil
}

func (s *State) Equals(s2 State) bool {
	return maps.EqualFunc(s.Identities, s2.Identities, func(v1, v2 Identity) bool {
		return v1.Equals(v2)
	}) && maps.EqualFunc(s.Roles, s2.Roles, func(v1, v2 Role) bool {
		return v1.Equals(v2)
	}) && s.Policy.Equals(s2.Policy)
}

func ConfigureDefaults(admins []inet256.Addr) func(x State) (*State, error) {
	return func(x State) (*State, error) {
		x.Identities = map[string]Identity{
			"admins":   NewUnionIden(slices2.Map(admins, func(peer PeerID) Identity { return NewPeer(peer) })...),
			"touchers": {},
			"lookers":  {},
		}
		x.Roles = map[string]Role{
			// admin can do everything on the host config branch
			"admin": NewRegexpRole(regexp.MustCompile(".*"), regexp.MustCompile(HostConfigKey)),
			// look can look at everything
			"look": NewUnionRole(
				slices2.Map([]string{
					branchintc.Verb_List,
					branchintc.Verb_Get,

					branchintc.Verb_ReadCell,

					branchintc.Verb_GetBlob,
					branchintc.Verb_ListBlob,
				}, func(v string) Role {
					return NewRegexpRole(regexp.MustCompile(v), regexp.MustCompile(".*"))
				})...),
			// touch can touch everything
			"touch": NewUnionRole(
				slices2.Map([]string{
					branchintc.Verb_Create,
					branchintc.Verb_Set,
					branchintc.Verb_Delete,

					branchintc.Verb_CASCell,
					branchintc.Verb_PostBlob,
					branchintc.Verb_DeleteBlob,
				}, func(v string) Role {
					return NewRegexpRole(regexp.MustCompile(v), regexp.MustCompile(".*"))
				})...),
		}
		x.Policy = Policy{
			Rules: []Rule{
				NewRule(NewNamedIden("admins"), NewUnionRole(
					NewNamedRole("admin"),
					NewNamedRole("touch"),
					NewNamedRole("look"),
				)),
				NewRule(NewNamedIden("touchers"), NewSubtractRole(
					NewNamedRole("touch"),
					NewNamedRole("admin"),
				)),
				NewRule(NewNamedIden("lookers"), NewSubtractRole(
					NewNamedRole("look"),
					NewNamedRole("admin"),
				)),
			},
		}
		return &x, nil
	}
}
