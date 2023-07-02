// Package gothost provides tools for configuring access permissions on the host.
package gothost

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"regexp"

	"github.com/brendoncarroll/go-exp/slices2"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/internal/graphs"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/inet256/inet256/pkg/inet256"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

const (
	// HostConfigKey is the branch key for the host config
	HostConfigKey = "__host__"

	IdentitiesPath = "IDENTITIES"
	PolicyPath     = "POLICY"
)

type PeerID = inet256.Addr

type State struct {
	Policy     Policy
	Identities map[string]Identity
}

func (s *State) Load(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, root gotfs.Root) error {
	// identities
	maps.Clear(s.Identities)
	if s.Identities == nil {
		s.Identities = make(map[string]Identity)
	}
	if err := fsop.ReadDir(ctx, ms, root, IdentitiesPath, func(e gotfs.DirEnt) error {
		data, err := fsop.ReadFile(ctx, ms, ds, root, path.Join(IdentitiesPath, e.Name), 1<<16)
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
	polData, err := fsop.ReadFile(ctx, ms, ds, root, PolicyPath, 1<<20)
	if err != nil && !posixfs.IsErrNotExist(err) {
		return err
	} else if err == nil {
		pol, err := ParsePolicy(polData)
		if err != nil {
			return err
		}
		s.Policy = *pol
	}
	return nil
}

func (s *State) Save(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store) (*gotfs.Root, error) {
	b := fsop.NewBuilder(ctx, ms, ds)
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

	return b.Finish()
}

func (s *State) Validate() error {
	cycle := graphs.FindCycle(maps.Keys(s.Identities), func(out []string, v string) []string {
		for _, elem := range s.Identities[v].Members {
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
	}) && s.Policy.Equals(s2.Policy)
}

func ConfigureDefaults(admins []inet256.Addr) func(x State) (*State, error) {
	return func(x State) (*State, error) {
		x.Identities = map[string]Identity{
			"admins": {
				Owners:  slices2.Map(admins, func(peer PeerID) IdentityElement { return NewPeer(peer) }),
				Members: slices2.Map(admins, func(peer PeerID) IdentityElement { return NewPeer(peer) }),
			},
			"touchers": {},
			"lookers":  {},
		}
		x.Policy = Policy{
			Rules: []Rule{
				NewRule(true, NewNamed("lookers"), OpLook, regexp.MustCompile(".*")),
				NewRule(true, NewNamed("touchers"), OpTouch, regexp.MustCompile(".*")),
				NewRule(true, NewNamed("admins"), OpTouch, regexp.MustCompile(".*")),

				// Only allow admins to access HostConfigKey
				NewRule(false, Anyone(), OpTouch, regexp.MustCompile(HostConfigKey)),
				NewRule(true, NewNamed("admins"), OpTouch, regexp.MustCompile(HostConfigKey)),
			},
		}
		return &x, nil
	}
}
