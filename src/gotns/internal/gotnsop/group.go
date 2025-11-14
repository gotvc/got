package gotnsop

import (
	"bytes"
	"fmt"

	"github.com/cloudflare/circl/kem"
	"github.com/gotvc/got/src/internal/sbe"
	"go.inet256.org/inet256/src/inet256"
)

type Group struct {
	// Name uniquely identifies the group, it is the primary key of the Groups table.
	Name string

	// KEM is used to send messages to the group.
	// The private key is stored encrypted in each Membership entry.
	KEM kem.PublicKey
	// Leaves are the leaves that are part of the group.
	// The key in the leaves map is the leaf ID.
	// The value in the leaves map is the group's secret key encrypted for the leaf to read.
	LeafKEMs map[inet256.ID][]byte
	// Owners are the identities that can add and remove members from the group.
	// Owners must also be members of the group.
	Owners []inet256.ID
}

func ParseGroup(key, value []byte) (*Group, error) {
	kemPubData, data, err := sbe.ReadLP(value)
	if err != nil {
		return nil, err
	}
	kemPub, err := ParseKEMPublicKey(kemPubData)
	if err != nil {
		return nil, err
	}
	// leaves
	leavesData, data, err := sbe.ReadLP(data)
	if err != nil {
		return nil, err
	}
	leaves := make(map[inet256.ID][]byte)
	if err := UnmarshalIDMap(leavesData, leaves); err != nil {
		return nil, err
	}
	// owners
	ownersData, _, err := sbe.ReadLP(data)
	if err != nil {
		return nil, err
	}
	var owners []inet256.ID
	if err := unmarshalGroupOwners(ownersData, &owners); err != nil {
		return nil, err
	}
	return &Group{
		Name:     string(key),
		KEM:      kemPub,
		LeafKEMs: leaves,
		Owners:   owners,
	}, nil
}

func (g *Group) Key(out []byte) []byte {
	return append(out, g.Name...)
}

func (g *Group) Value(out []byte) []byte {
	out = sbe.AppendLP(out, MarshalKEMPublicKey(nil, KEM_MLKEM1024, g.KEM))
	out = sbe.AppendLP(out, MarshalIDMap(nil, g.LeafKEMs))
	out = sbe.AppendLP(out, marshalGroupOwners(nil, g.Owners))
	return out
}

func compareLeafIDs(a, b inet256.ID) int {
	return bytes.Compare(a[:], b[:])
}

func marshalGroupOwners(out []byte, owners []inet256.ID) []byte {
	for _, owner := range owners {
		out = append(out, owner[:]...)
	}
	return out
}

func unmarshalGroupOwners(data []byte, dst *[]inet256.ID) error {
	if len(data)%32 != 0 {
		return fmt.Errorf("invalid group owners data")
	}
	for i := 0; i < len(data); i += 32 {
		*dst = append(*dst, inet256.IDFromBytes(data[i:i+32]))
	}
	return nil
}
