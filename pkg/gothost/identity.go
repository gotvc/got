package gothost

import (
	"bytes"
	"context"
	"path"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/inet256/inet256/pkg/inet256"
	"golang.org/x/exp/maps"
)

type Identity struct {
	Owners []IdentityElement `json:"owners"`
	Actors []IdentityElement `json:"actors"`
}

type IdentityElement struct {
	Peer *PeerID
	Name *string
}

func (e IdentityElement) String() string {
	if e.Peer != nil {
		return e.Peer.Base64String()
	}
	if e.Name != nil {
		return "@" + *e.Name
	}
	return "IdentityElement{}"
}

func CreateIdentity(ctx context.Context, fsop *gotfs.Operator, ms, ds cadata.Store, x gotfs.Root, name string) (*gotfs.Root, error) {
	root := &x
	root, err := fsop.MkdirAll(ctx, ms, *root, IdentitiesPath)
	if err != nil {
		return nil, err
	}
	p := path.Join(IdentitiesPath, name)
	return fsop.CreateFile(ctx, ms, ds, *root, p, bytes.NewReader(nil))
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

// FlattenIdentity lists the elements of an identity
func FlattenIdentity(ctx context.Context, fsop *gotfs.Operator, ms cadata.Store, root gotfs.Root, iden string) ([]PeerID, error) {
	if !strings.HasPrefix(iden, "@") {
		peer, err := inet256.ParseAddrBase64([]byte(iden))
		if err != nil {
			return nil, err
		}
		return []PeerID{peer}, nil
	}
	// TODO: recursively list elements
	peers := make(map[PeerID]struct{})
	return maps.Keys(peers), nil
}
