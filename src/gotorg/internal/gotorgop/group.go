package gotorgop

import (
	"bytes"
	"fmt"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/cloudflare/circl/kem"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/sbe"
	"go.inet256.org/inet256/src/inet256"
)

// Name uniquely identifies the group, it is the primary key of the Groups table.
type GroupID = blobcache.CID

// ComputeGroupID computes the ID of a group from a nonce and initial owners.
// This value must be unique across all groups in the namespace.
// The GroupID does not change over the lifetime of the group, even though the owners might.
func ComputeGroupID(nonce [16]byte, initOwners IDSet) GroupID {
	var data []byte
	data = append(data, nonce[:]...)
	data = append(data, initOwners.Marshal(nil)...)
	return gdat.Hash(data)
}

func ParseGroupID(x []byte) (GroupID, error) {
	if len(x) != blobcache.CIDSize {
		return GroupID{}, fmt.Errorf("invalid group id: length is not %d: %d", blobcache.CIDSize, len(x))
	}
	return GroupID(x), nil
}

type Group struct {
	// ID uniquely identifies the group.
	// It is stored in the key,
	ID GroupID

	// All the fields below are stored in the value.

	// Nonce is random additional data used to compute the GroupID
	Nonce [16]byte
	// KEM is used to send messages to the group.
	// The private key is stored encrypted in each Membership entry.
	KEM kem.PublicKey
	// Owners are the identities that can add and remove members from the group.
	// Owners must also be members of the group.
	Owners IDSet
}

func ParseGroup(key, value []byte) (*Group, error) {
	gid, err := ParseGroupID(key)
	if err != nil {
		return nil, err
	}

	if len(value) < 16 {
		return nil, nil
	}
	nonce, value := [16]byte(value[:16]), value[16:]
	// KEM
	kemPubData, data, err := sbe.ReadLP(value)
	if err != nil {
		return nil, err
	}
	kemPub, err := ParseKEMPublicKey(kemPubData)
	if err != nil {
		return nil, err
	}
	// owners
	ownersData, _, err := sbe.ReadLP(data)
	if err != nil {
		return nil, err
	}
	var owners IDSet
	if err := owners.Unmarshal(ownersData); err != nil {
		return nil, err
	}
	return &Group{
		ID:     gid,
		Nonce:  nonce,
		KEM:    kemPub,
		Owners: owners,
	}, nil
}

func (g *Group) Key(out []byte) []byte {
	return append(out, g.ID[:]...)
}

func (g *Group) Value(out []byte) []byte {
	out = append(out, g.Nonce[:]...)
	out = sbe.AppendLP(out, MarshalKEMPublicKey(nil, KEM_MLKEM1024, g.KEM))
	out = sbe.AppendLP(out, g.Owners.Marshal(nil))
	return out
}

func compareLeafIDs(a, b inet256.ID) int {
	return bytes.Compare(a[:], b[:])
}

// IDSet is a set of inet256.ID
// It has a deterministic wire format.
type IDSet []inet256.ID

func (s *IDSet) Contains(id inet256.ID) bool {
	_, found := slices.BinarySearchFunc(*s, id, compareLeafIDs)
	return found
}

func (s *IDSet) Add(id inet256.ID) {
	idx, found := slices.BinarySearchFunc(*s, id, compareLeafIDs)
	if found {
		return
	}
	*s = slices.Insert(*s, idx, id)
}

func (s *IDSet) Remove(id inet256.ID) {
	idx, found := slices.BinarySearchFunc(*s, id, compareLeafIDs)
	if !found {
		return
	}
	*s = slices.Delete(*s, idx, idx+1)
}

func (s *IDSet) Len() int {
	return len(*s)
}

func (s IDSet) Marshal(out []byte) []byte {
	for _, id := range s {
		out = append(out, id[:]...)
	}
	return out
}

func (s *IDSet) Unmarshal(data []byte) error {
	if len(data)%32 != 0 {
		return fmt.Errorf("invalid id set data: length is not a multiple of 32: %d", len(data))
	}
	s2 := make([]inet256.ID, 0, len(data)/32)
	for i := 0; i < len(data); i += 32 {
		s2 = append(s2, inet256.IDFromBytes(data[i:i+32]))
	}
	if !slices.IsSortedFunc(s2, func(a, b inet256.ID) int {
		return bytes.Compare(a[:], b[:])
	}) {
		return fmt.Errorf("ids are not sorted")
	}
	*s = s2
	return nil
}

// Member is a member of a group.
type Member struct {
	// Unit is a single identity unit.
	Unit *inet256.ID
	// Group is a reference to another group by that group's ID.
	Group *GroupID
}

func MemberUnit(id inet256.ID) Member {
	return Member{
		Unit: &id,
	}
}

func MemberGroup(id GroupID) Member {
	return Member{
		Group: &id,
	}
}
func (m Member) Marshal(out []byte) []byte {
	switch {
	case m.Unit != nil:
		out = append(out, 0)
		out = append(out, m.Unit[:]...)
	case m.Group != nil:
		out = append(out, 1)
		out = append(out, m.Group[:]...)
	default:
		panic("member is empty")
	}
	return out
}

func (m *Member) Unmarshal(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("member is too short to contain discriminant")
	}
	discrim, data := data[0], data[1:]
	switch discrim {
	case 0:
		if len(data) != 32 {
			return fmt.Errorf("member is too short to contain unit ID")
		}
		unit := inet256.IDFromBytes(data)
		m.Unit = &unit
	case 1:
		sgid, err := ParseGroupID(data)
		if err != nil {
			return err
		}
		m.Group = &sgid
	default:
		return fmt.Errorf("invalid member discriminator: %q", discrim)
	}
	return nil
}
