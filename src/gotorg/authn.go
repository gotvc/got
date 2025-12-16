package gotorg

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/sign"
	"go.inet256.org/inet256/src/inet256"
	"golang.org/x/crypto/chacha20"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotorg/internal/gotorgop"
	"github.com/gotvc/got/src/internal/graphs"
	"github.com/gotvc/got/src/internal/stores"
)

type (
	IdentityUnit = gotorgop.IdentityUnit
	Group        = gotorgop.Group
	GroupID      = gotorgop.GroupID
)

func MemberUnit(id inet256.ID) gotorgop.Member {
	return gotorgop.MemberUnit(id)
}

func MemberGroup(gid GroupID) gotorgop.Member {
	return gotorgop.MemberGroup(gid)
}

// PutLeaf adds a leaf to the leaves table, overwriting whatever was there.
// Any unreferenced leaves will be deleted.
func (m *Machine) PutIDUnit(ctx context.Context, s stores.Writing, state State, leaf IdentityUnit) (*State, error) {
	leafState, err := m.gotkv.Mutate(ctx, s.(stores.RW), state.IDUnits, putLeaf(leaf))
	if err != nil {
		return nil, err
	}
	state.IDUnits = *leafState
	return &state, nil
}

// GetIDUnit retuns an identity unit by ID.
func (m *Machine) GetIDUnit(ctx context.Context, s stores.Reading, state State, id inet256.ID) (*IdentityUnit, error) {
	val, err := m.gotkv.Get(ctx, s, state.IDUnits, id[:])
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return gotorgop.ParseIDUnit(id[:], val)
}

// DropIDUnit drops a unit from the units table.
func (m *Machine) DropIDUnit(ctx context.Context, s stores.RW, state State, id inet256.ID) (*State, error) {
	if err := m.ForEachMembership(ctx, s, state, nil, func(mem Membership) error {
		if mem.Member.Unit != nil && *mem.Member.Unit == id {
			return fmt.Errorf("leaf is still in group")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	leafState, err := m.gotkv.Delete(ctx, s, state.IDUnits, id[:])
	if err != nil {
		return nil, err
	}
	state.IDUnits = *leafState
	return &state, nil
}

func (m *Machine) ForEachIDUnit(ctx context.Context, s stores.Reading, state State, fn func(unit IdentityUnit) error) error {
	span := gotkv.TotalSpan()
	return m.gotkv.ForEach(ctx, s, state.IDUnits, span, func(ent gotkv.Entry) error {
		unit, err := gotorgop.ParseIDUnit(ent.Key, ent.Value)
		if err != nil {
			return err
		}
		return fn(*unit)
	})
}

// PutGroup adds or replaces a group by name.
func (m *Machine) PutGroup(ctx context.Context, s stores.RW, state State, group Group) (*State, error) {
	groupState, err := m.gotkv.Mutate(ctx, s, state.Groups, putGroup(group))
	if err != nil {
		return nil, err
	}
	state.Groups = *groupState
	return &state, nil
}

// GetGroup returns a group by name.
func (m *Machine) GetGroup(ctx context.Context, s stores.Reading, State State, groupID GroupID) (*Group, error) {
	k := groupID[:]
	val, err := m.gotkv.Get(ctx, s, State.Groups, k)
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return gotorgop.ParseGroup(k, val)
}

// ForEachGroup calls fn for each group in the namespace.
func (m *Machine) ForEachGroup(ctx context.Context, s stores.Reading, state State, fn func(group gotorgop.Group) error) error {
	span := gotkv.TotalSpan()
	return m.gotkv.ForEach(ctx, s, state.Groups, span, func(ent gotkv.Entry) error {
		group, err := gotorgop.ParseGroup(ent.Key, ent.Value)
		if err != nil {
			return err
		}
		return fn(*group)
	})
}

func (m *Machine) PutGroupName(ctx context.Context, s stores.RW, state State, name string, groupID gotorgop.GroupID) (*State, error) {
	_, err := m.GetGroup(ctx, s, state, groupID)
	if err != nil {
		return nil, err
	}
	if err := m.mapKV(ctx, s, &state.GroupNames, func(tx *gotkv.Tx) error {
		return tx.Put(ctx, []byte(name), groupID[:])
	}); err != nil {
		return nil, err
	}
	return &state, nil
}

func (m *Machine) GetGroupName(ctx context.Context, s stores.Reading, x State, name string) (*gotorgop.GroupID, error) {
	var groupID *gotorgop.GroupID
	if err := m.gotkv.GetF(ctx, s, x.GroupNames, []byte(name), func(val []byte) error {
		id, err := gotorgop.ParseGroupID(val)
		if err != nil {
			return err
		}
		groupID = &id
		return nil
	}); err != nil {
		return nil, err
	}
	return groupID, nil
}

func (m *Machine) LookupGroup(ctx context.Context, s stores.Reading, state State, name string) (*Group, error) {
	groupID, err := m.GetGroupName(ctx, s, state, name)
	if err != nil {
		return nil, err
	}
	return m.GetGroup(ctx, s, state, *groupID)
}

func (m *Machine) GetMembership(ctx context.Context, s stores.Reading, state State, group GroupID, mem Member) (*Membership, error) {
	k := memberKey(nil, group, mem)
	val, err := m.gotkv.Get(ctx, s, state.Memberships, k)
	if err != nil {
		return nil, err
	}
	return ParseMembership(k, val)
}

// ForEachMembership calls fn for each membership
// If gid is non-nil, then it will be used to filter by the containing group.
func (m *Machine) ForEachMembership(ctx context.Context, s stores.Reading, state State, gid *GroupID, fn func(mem Membership) error) error {
	span := gotkv.TotalSpan()
	return m.gotkv.ForEach(ctx, s, state.Memberships, span, func(ent gotkv.Entry) error {
		mem, err := ParseMembership(ent.Key, ent.Value)
		if err != nil {
			return err
		}
		if gid != nil && *gid != mem.Group {
			return nil
		}
		return fn(*mem)
	})
}

// AddMember adds a member (a unit or another Group) to a containing group.
func (m *Machine) AddMember(ctx context.Context, s stores.RW, x State, gid GroupID, member Member, groupSecret *gotorgop.Secret) (*State, error) {
	_, err := m.GetGroup(ctx, s, x, gid)
	if err != nil {
		return nil, err
	}
	var kemPub kem.PublicKey
	switch {
	case member.Unit != nil:
		idUnit, err := m.GetIDUnit(ctx, s, x, *member.Unit)
		if err != nil {
			return nil, err
		}
		if idUnit == nil {
			return nil, fmt.Errorf("cannot create membership identity does not exist: %v", *member.Unit)
		}
		kemPub = idUnit.KEMPublicKey
	case member.Group != nil:
		g, err := m.GetGroup(ctx, s, x, *member.Group)
		if err != nil {
			return nil, err
		}
		if g == nil {
			return nil, fmt.Errorf("cannot create membership group does not exist: %v", *member.Group)
		}
		kemPub = g.KEM
	default:
		return nil, fmt.Errorf("member is empty")
	}

	kemCtext := encryptSeed(nil, kemPub, groupSecret)
	memState, err := m.gotkv.Mutate(ctx, s, x.Memberships,
		putMember(Membership{
			Group:        gid,
			Member:       member,
			EncryptedKEM: kemCtext,
		}),
	)
	if err != nil {
		return nil, err
	}
	x.Memberships = *memState
	return &x, nil
}

// RemoveMember removes a member group from a containing group.
func (m *Machine) RemoveMember(ctx context.Context, s stores.RW, state State, gid GroupID, mem Member) (*State, error) {
	// TODO: Need to make sure group exists first.
	memState, err := m.gotkv.Mutate(ctx, s, state.Memberships,
		rmMember(gid, mem),
	)
	if err != nil {
		return nil, err
	}
	state.Memberships = *memState
	return &state, nil
}

// ForEachInGroup calls fn recursively for each ID in the group.
// This is a recursive method which explores the full transitive closure of the initial Group.
func (m *Machine) ForEachUnitInGroup(ctx context.Context, s stores.Reading, state State, gid GroupID, fn func(inet256.ID) error) error {
	return m.ForEachMembership(ctx, s, state, &gid, func(mem Membership) error {
		switch {
		case mem.Member.Unit != nil:
			return fn(*mem.Member.Unit)
		case mem.Member.Group != nil:
			return m.ForEachUnitInGroup(ctx, s, state, *mem.Member.Group, fn)
		}
		return nil
	})
}

// GroupContains returns true if the actor is a member of the group.
// This method answers the `contains?` query for the full transitive closure for the group.
// For immediate memberhsip call GetMembership directly and check for (nil, nil);
// that means the memberhip does not exist.
func (m *Machine) GroupContains(ctx context.Context, s stores.Reading, state State, group GroupID, actor inet256.ID) (bool, error) {
	var contains bool
	stopEarly := errors.New("stop early")
	if err := m.ForEachUnitInGroup(ctx, s, state, group, func(id inet256.ID) error {
		if actor == id {
			contains = true
			return stopEarly
		}
		return nil
	}); err != nil && !errors.Is(err, stopEarly) {
		return false, err
	}
	return contains, nil
}

// GetGroupSecret returns a KEM seed used to derive the key pair for a given group.
// id is the ID of the leaf that is requesting the KEM private key.
// kemPriv is the KEM private key for the leaf to decrypt messages sent to it by group operations.
// groupPath should go from the largest group to the smallest group.
// If you need a groupPath, try `FindGroupPath` first.
func (m *Machine) GetGroupSecret(ctx context.Context, s stores.Reading, state State, priv IdenPrivate, groupPath []GroupID) (*gotorgop.Secret, error) {
	kemPriv := priv.KEMPrivateKey
	var groupSecret *gotorgop.Secret

	memk := MemberUnit(priv.GetID())
	for len(groupPath) > 0 {
		gid := groupPath[len(groupPath)-1]
		groupPath = groupPath[:len(groupPath)-1]
		group, err := m.GetGroup(ctx, s, state, gid)
		if err != nil {
			return nil, err
		}
		mshp, err := m.GetMembership(ctx, s, state, gid, memk)
		if err != nil {
			return nil, err
		}

		gs, err := decryptSeed(kemPriv, mshp.EncryptedKEM)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt KEM seed: %w", err)
		}
		kemPub, _ := gs.DeriveKEM()
		if !kemPub.Equal(group.KEM) {
			return nil, fmt.Errorf("secret from membership ctext did not produce group KEM")
		}
		groupSecret = gs
		_, kemPriv = groupSecret.DeriveKEM()
	}
	if groupSecret == nil {
		return nil, fmt.Errorf("group secret not found")
	}
	return groupSecret, nil
}

// RekeyGroup creates a new KEM key pair for a group and
// - changes the group's KEM public key
// - re-encrypts the KEM private key for all leaves, using the leaves' KEM public key
// - re-encrypts the KEM private key for all member groups, using those groups' KEM public keys
func (m *Machine) RekeyGroup(ctx context.Context, s stores.RW, state State, gid GroupID, secret *gotorgop.Secret) (*State, error) {
	group, err := m.GetGroup(ctx, s, state, gid)
	if err != nil {
		return nil, err
	}

	kemPub, _ := secret.DeriveKEM()
	// update group record
	group.KEM = kemPub

	// update memberships
	var memMuts []gotkv.Mutation
	if err := m.ForEachMembership(ctx, s, state, &gid, func(mshp Membership) error {
		mem := mshp.Member
		switch {
		case mem.Unit != nil:
			// TODO
			panic("not implemented")

		case mem.Group != nil:
			subgroup, err := m.GetGroup(ctx, s, state, *mem.Group)
			if err != nil {
				return err
			}
			mshp.EncryptedKEM = encryptSeed(nil, subgroup.KEM, secret)
			memMuts = append(memMuts, putMember(mshp))
		default:
			return fmt.Errorf("member is empty")
		}
		return nil
	}); err != nil {
		return nil, err
	}

	memRoot, err := m.gotkv.Mutate(ctx, s, state.Memberships, memMuts...)
	if err != nil {
		return nil, err
	}
	state.Memberships = *memRoot

	return &state, nil
}

// FindGroupPath finds a path of groups from priv to the target Group.
// FindGroupPath returns (nil, nil) when no path could be found.
func (m *Machine) FindGroupPath(ctx context.Context, s stores.Reading, x State, id inet256.ID, target GroupID) ([]GroupID, error) {
	initial := []GroupID{}
	// List all the groups containing the unit
	if err := m.ForEachGroup(ctx, s, x, func(g Group) error {
		mem := MemberUnit(id)
		mshp, err := m.GetMembership(ctx, s, x, g.ID, mem)
		if err != nil {
			return err
		}
		if mshp == nil {
			return nil
		}
		initial = append(initial, g.ID)
		return nil
	}); err != nil {
		return nil, err
	}

	goal := func(gid GroupID) bool { return gid == target }
	return graphs.DijkstrasErr(initial, goal, func(gid GroupID) iter.Seq2[GroupID, error] {
		return func(yield func(GroupID, error) bool) {
			stopIter := errors.New("stop iter")
			err := m.ForEachMembership(ctx, s, x, &gid, func(mshp Membership) error {
				switch {
				case mshp.Member.Group != nil:
					if !yield(*mshp.Member.Group, nil) {
						return stopIter
					}
				}
				return nil
			})
			if err != nil {
				if errors.Is(err, stopIter) {
					return
				} else {
					yield(GroupID{}, err)
				}
			}
		}
	})
}

type IdenPrivate struct {
	SigPrivateKey inet256.PrivateKey
	KEMPrivateKey kem.PrivateKey
}

func (iden *IdenPrivate) GetID() inet256.ID {
	pubKey := iden.SigPrivateKey.Public().(sign.PublicKey)
	return pki.NewID(pubKey)
}

func (iden *IdenPrivate) Public() IdentityUnit {
	return IdentityUnit{
		ID:           iden.GetID(),
		SigPublicKey: iden.SigPrivateKey.Public().(sign.PublicKey),
		KEMPublicKey: iden.KEMPrivateKey.Public(),
	}
}

type Member = gotorgop.Member

// Membership contains the Group's KEM seed encrypted for the target member.
type Membership struct {
	Group  GroupID
	Member Member

	// EncryptedKEM contains a KEM ciphertext, and a symmetric ciphertext.
	// The symmetric ciphertext contains the KEM seed for the Group's KEM private key.
	EncryptedKEM []byte
}

func ParseMembership(k, v []byte) (*Membership, error) {
	group, member, err := parseMemberKey(k)
	if err != nil {
		return nil, err
	}
	return &Membership{
		Group:        group,
		Member:       member,
		EncryptedKEM: v,
	}, nil
}

func (m *Membership) Key(out []byte) []byte {
	return memberKey(out, m.Group, m.Member)
}

func (m *Membership) Value(out []byte) []byte {
	return append(out, m.EncryptedKEM...)
}

func memberKey(key []byte, gid GroupID, member Member) []byte {
	key = append(key, gid[:]...)
	key = member.Marshal(key)
	return key
}

func parseMemberKey(key []byte) (GroupID, Member, error) {
	if len(key) < 32+32+1 {
		return GroupID{}, Member{}, fmt.Errorf("wrong size for membership key. %d", len(key))
	}
	gid := GroupID(key[:32])
	key = key[32:]
	var mem Member
	if err := mem.Unmarshal(key); err != nil {
		return GroupID{}, Member{}, err
	}
	return gid, mem, nil
}

// putMember returns a mutation that adds a member to a group.
func putMember(mem Membership) gotkv.Mutation {
	key := memberKey(nil, mem.Group, mem.Member)
	return gotkv.Mutation{
		Span: gotkv.SingleKeySpan(key),
		Entries: []gotkv.Entry{
			{
				Key:   key,
				Value: mem.Value(nil),
			},
		},
	}
}

// rmMember removes a member from a group.
func rmMember(gid GroupID, member Member) gotkv.Mutation {
	key := memberKey(nil, gid, member)
	return gotkv.Mutation{
		Span:    gotkv.SingleKeySpan(key),
		Entries: []gotkv.Entry{},
	}
}

// putLeaf returns a gotkv mutation that adds a leaf to the leaves table.
func putLeaf(leaf IdentityUnit) gotkv.Mutation {
	key := leaf.Key(nil)
	return gotkv.Mutation{
		Span: gotkv.SingleKeySpan(key),
		Entries: []gotkv.Entry{
			{Key: key, Value: leaf.Value(nil)},
		},
	}
}

// encryptSeed encryptes a secret seed
func encryptSeed(out []byte, recvKEM kem.PublicKey, secretSeed *gotorgop.Secret) []byte {
	kemCtext, ss, err := recvKEM.Scheme().Encapsulate(recvKEM)
	if err != nil {
		panic(err)
	}
	out = append(out, kemCtext...)
	out = appendXOR(out, (*[32]byte)(ss), secretSeed[:])
	return out
}

// decryptSeed decrypts a secret seed
func decryptSeed(recvKEM kem.PrivateKey, ctext []byte) (*gotorgop.Secret, error) {
	kemCtextSize := recvKEM.Scheme().CiphertextSize()
	if len(ctext) < kemCtextSize {
		return nil, fmt.Errorf("ctext too short to contain KEM ciphertext")
	}
	kemCtext := ctext[:kemCtextSize]
	ss, err := recvKEM.Scheme().Decapsulate(recvKEM, kemCtext)
	if err != nil {
		return nil, err
	}
	var seed gotorgop.Secret
	copy(seed[:], ctext[kemCtextSize:])
	cryptoXOR((*[32]byte)(ss), seed[:], seed[:])
	return &seed, nil
}

// cryptoXOR runs a chacha20 stream cipher over src, writing the result to dst.
func cryptoXOR(key *[32]byte, dst, src []byte) {
	var nonce [12]byte
	cipher, err := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if err != nil {
		panic(err)
	}
	cipher.XORKeyStream(dst, src)
}

// appendXOR appends src XOR'd with a chacha20 stream cipher to out.
func appendXOR(out []byte, key *[32]byte, src []byte) []byte {
	offset := len(out)
	out = append(out, src...)
	cryptoXOR(key, out[offset:], out[offset:])
	return out
}

func hashOfKEM(kem kem.PublicKey) [32]byte {
	return [32]byte(gdat.Hash(MarshalKEMPublicKey(nil, KEM_MLKEM1024, kem)))
}

const (
	KEM_MLKEM1024 = "mlkem1024"
)

var pki = gotorgop.PKI()

func PKI() inet256.PKI {
	return gotorgop.PKI()
}

func NewIDUnit(pubKey inet256.PublicKey, kemPub kem.PublicKey) IdentityUnit {
	return gotorgop.NewIDUnit(pubKey, kemPub)
}

func MarshalKEMPublicKey(out []byte, tag string, kem kem.PublicKey) []byte {
	return gotorgop.MarshalKEMPublicKey(out, tag, kem)
}

func MarshalKEMPrivateKey(out []byte, tag string, kem kem.PrivateKey) []byte {
	return gotorgop.MarshalKEMPrivateKey(out, tag, kem)
}

func ParseKEMPrivateKey(data []byte) (kem.PrivateKey, error) {
	return gotorgop.ParseKEMPrivateKey(data)
}

func ParseKEMPublicKey(data []byte) (kem.PublicKey, error) {
	return gotorgop.ParseKEMPublicKey(data)
}
