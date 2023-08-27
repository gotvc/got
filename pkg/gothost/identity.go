package gothost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"reflect"
	"slices"
	"strings"

	"github.com/brendoncarroll/go-exp/slices2"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/inet256/inet256/pkg/inet256"
)

// Identity is a set of peer addresses
type Identity struct {
	Peer   *PeerID    `json:"peer,omitempty"`
	Name   *string    `json:"name,omitempty"`
	Anyone *struct{}  `json:"anyone,omitempty"`
	Union  []Identity `json:"union,omitempty"`
}

func (i Identity) Add(xs ...Identity) Identity {
	switch {
	case i.Peer != nil:
		return NewUnionIden(append(xs, i)...)
	case i.Name != nil:
		return NewUnionIden(append(xs, i)...)
	case i.Anyone != nil:
		return i
	case i.Union != nil:
		return NewUnionIden(append(xs, i.Union...)...)
	default:
		return NewUnionIden(xs...)
	}
}

func (i Identity) Remove(xs ...Identity) Identity {
	switch {
	case i.Union != nil:
		return Identity{
			Union: slices2.Filter(i.Union, func(iden Identity) bool {
				return !slices.ContainsFunc(xs, func(x Identity) bool {
					return x.Equals(iden)
				})
			}),
		}
	default:
		return i
	}
}

func (i Identity) Equals(j Identity) bool {
	return reflect.DeepEqual(i, j)
}

func (e Identity) String() string {
	if e.Peer != nil {
		return e.Peer.Base64String()
	}
	if e.Name != nil {
		return "@" + *e.Name
	}
	if e.Anyone != nil {
		return "ANYONE"
	}
	if e.Union != nil {
		return "[" + strings.Join(slices2.Map(e.Union, func(x Identity) string { return x.String() }), ", ") + "]"
	}
	return "NOONE"
}

// NewPeer returns a single PeerID element.
func NewPeer(peer PeerID) Identity {
	return Identity{Peer: &peer}
}

// NewNamed returns a reference to another identity.
func NewNamedIden(name string) Identity {
	return Identity{Name: &name}
}

// Anyone returns an element that includes anyone
func Anyone() Identity {
	return Identity{Anyone: new(struct{})}
}

func NewUnionIden(idens ...Identity) Identity {
	if len(idens) == 0 {
		return Identity{}
	}
	if len(idens) == 1 {
		return idens[0]
	}
	idens = slices.Clone(idens)
	slices.SortFunc(idens, func(a, b Identity) int {
		return strings.Compare(a.String(), b.String())
	})
	// TODO dedup
	return Identity{Union: idens}
}

// ParseIDElement parses an IdentityElement from it's text representation.
func ParseIDElement(x []byte) (Identity, error) {
	switch {
	case bytes.Equal(x, []byte("ANYONE")):
		return Anyone(), nil
	case bytes.HasPrefix(x, []byte("@")):
		return NewNamedIden(string(bytes.TrimPrefix(x, []byte("@")))), nil
	default:
		if addr, err := inet256.ParseAddrBase64(x); err == nil {
			return NewPeer(addr), nil
		}
		return Identity{}, fmt.Errorf("could not parse identity element from %q", x)
	}
}

func CreateIdentity(ctx context.Context, fsag *gotfs.Agent, ms, ds cadata.Store, x gotfs.Root, name string, iden Identity) (*gotfs.Root, error) {
	root := &x
	root, err := fsag.MkdirAll(ctx, ms, *root, IdentitiesPath)
	if err != nil {
		return nil, err
	}
	p := path.Join(IdentitiesPath, name)
	data, err := json.Marshal(iden)
	if err != nil {
		return nil, err
	}
	return fsag.CreateFile(ctx, ms, ds, *root, p, bytes.NewReader(data))
}

func DeleteIdentity(ctx context.Context, fsag *gotfs.Agent, ms cadata.Store, x gotfs.Root, name string) (*gotfs.Root, error) {
	root := &x
	root, err := fsag.MkdirAll(ctx, ms, *root, IdentitiesPath)
	if err != nil {
		return nil, err
	}
	p := path.Join(IdentitiesPath, name)
	return fsag.RemoveAll(ctx, ms, *root, p)
}

func GetIdentity(ctx context.Context, fsag *gotfs.Agent, ms, ds cadata.Store, root gotfs.Root, name string) (*Identity, error) {
	p := path.Join(IdentitiesPath, name)
	r, err := fsag.NewReader(ctx, ms, ds, root, p)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var ret Identity
	if err := json.Unmarshal(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func ListIdentities(ctx context.Context, fsag *gotfs.Agent, ms cadata.Store, root gotfs.Root) ([]string, error) {
	var ret []string
	if err := fsag.ReadDir(ctx, ms, root, IdentitiesPath, func(ent gotfs.DirEnt) error {
		ret = append(ret, ent.Name)
		return nil
	}); err != nil {
		if posixfs.IsErrNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return ret, nil
}

type IDEntry struct {
	Name     string
	Identity Identity
}

func ListIdentitiesFull(ctx context.Context, fsag *gotfs.Agent, ms, ds cadata.Store, root gotfs.Root) ([]IDEntry, error) {
	var ret []IDEntry
	if err := fsag.ReadDir(ctx, ms, root, IdentitiesPath, func(ent gotfs.DirEnt) error {
		r, err := fsag.NewReader(ctx, ms, ds, root, path.Join(IdentitiesPath, ent.Name))
		if err != nil {
			return err
		}
		// TODO: need maximum size
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		var iden Identity
		if err := json.Unmarshal(data, &iden); err != nil {
			return err
		}
		ret = append(ret, IDEntry{
			Name:     ent.Name,
			Identity: iden,
		})
		return nil
	}); err != nil {
		if posixfs.IsErrNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return ret, nil
}
