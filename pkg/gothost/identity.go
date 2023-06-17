package gothost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/inet256/inet256/pkg/inet256"
)

// Identity is 2 groups of peers.
// - Members are the set of peers that are part of the identity.
// - Owners are the set of peers that are allowed to modify the identity.
type Identity struct {
	Owners  []IdentityElement `json:"owners"`
	Members []IdentityElement `json:"members"`
}

// IdentityElement is a single element of an Identity, it can either refer to:
// - A single PeerID
// - Another Identity by name
// - Anyone
type IdentityElement struct {
	Peer   *PeerID   `json:"peer,omitempty"`
	Name   *string   `json:"name,omitempty"`
	Anyone *struct{} `json:"anyone,omitempty"`
}

// NewPeer returns a single PeerID element.
func NewPeer(peer PeerID) IdentityElement {
	return IdentityElement{Peer: &peer}
}

// NewNamed returns a reference to another identity.
func NewNamed(name string) IdentityElement {
	return IdentityElement{Name: &name}
}

// Anyone returns an element that includes anyone
func Anyone() IdentityElement {
	return IdentityElement{Anyone: new(struct{})}
}

// ParseIDElement parses an IdentityElement from it's text representation.
func ParseIDElement(x []byte) (IdentityElement, error) {
	switch {
	case bytes.Equal(x, []byte("ANYONE")):
		return Anyone(), nil
	case bytes.HasPrefix(x, []byte("@")):
		return NewNamed(string(bytes.TrimPrefix(x, []byte("@")))), nil
	default:
		if addr, err := inet256.ParseAddrBase64(x); err == nil {
			return NewPeer(addr), nil
		}
		return IdentityElement{}, fmt.Errorf("could not parse identity element from %q", x)
	}
}

func (e IdentityElement) String() string {
	if e.Peer != nil {
		return e.Peer.Base64String()
	}
	if e.Name != nil {
		return "@" + *e.Name
	}
	if e.Anyone != nil {
		return "ANYONE"
	}
	return "NOONE"
}

func CreateIdentity(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, x gotfs.Root, name string, iden Identity) (*gotfs.Root, error) {
	root := &x
	root, err := fsop.MkdirAll(ctx, ms, *root, IdentitiesPath)
	if err != nil {
		return nil, err
	}
	p := path.Join(IdentitiesPath, name)
	data, err := json.Marshal(iden)
	if err != nil {
		return nil, err
	}
	return fsop.CreateFile(ctx, ms, ds, *root, p, bytes.NewReader(data))
}

func DeleteIdentity(ctx context.Context, fsop *gotfs.Operator, ms cadata.Store, x gotfs.Root, name string) (*gotfs.Root, error) {
	root := &x
	root, err := fsop.MkdirAll(ctx, ms, *root, IdentitiesPath)
	if err != nil {
		return nil, err
	}
	p := path.Join(IdentitiesPath, name)
	return fsop.RemoveAll(ctx, ms, *root, p)
}

func GetIdentity(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, root gotfs.Root, name string) (*Identity, error) {
	p := path.Join(IdentitiesPath, name)
	r, err := fsop.NewReader(ctx, ms, ds, root, p)
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

func ListIdentities(ctx context.Context, fsop *gotfs.Operator, ms cadata.Store, root gotfs.Root) ([]string, error) {
	var ret []string
	if err := fsop.ReadDir(ctx, ms, root, IdentitiesPath, func(ent gotfs.DirEnt) error {
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

func ListIdentitiesFull(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, root gotfs.Root) ([]IDEntry, error) {
	var ret []IDEntry
	if err := fsop.ReadDir(ctx, ms, root, IdentitiesPath, func(ent gotfs.DirEnt) error {
		r, err := fsop.NewReader(ctx, ms, ds, root, path.Join(IdentitiesPath, ent.Name))
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
