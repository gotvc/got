package gotns

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/kem/mlkem/mlkem1024"
	"go.inet256.org/inet256/src/inet256"
	"golang.org/x/crypto/chacha20"
	"golang.org/x/crypto/chacha20poly1305"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
)

const MaxLeavesPerGroup = 128

// PutLeaf adds a leaf to the leaves table, overwriting whatever was there.
// Any unreferenced leaves will be deleted.
func (m *Machine) PutLeaf(ctx context.Context, s stores.RW, State State, leaf IdentityLeaf) (*State, error) {
	leafState, err := m.gotkv.Mutate(ctx, s, State.Leaves, putLeaf(leaf))
	if err != nil {
		return nil, err
	}
	State.Leaves = *leafState
	return &State, nil
}

// GetLeaf retuns an identity leaf by ID.
func (m *Machine) GetLeaf(ctx context.Context, s stores.Reading, State State, id inet256.ID) (*IdentityLeaf, error) {
	val, err := m.gotkv.Get(ctx, s, State.Leaves, id[:])
	if err != nil {
		return nil, err
	}
	return ParseIdentityLeaf(id[:], val)
}

// DropLeaf drops a leaf from the leaves table.
func (m *Machine) DropLeaf(ctx context.Context, s stores.RW, state State, leafID inet256.ID) (*State, error) {
	if err := m.ForEachGroup(ctx, s, state, func(group Group) error {
		if group.LeafKEMs[leafID] != nil {
			return fmt.Errorf("leaf is still in group")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	leafState, err := m.gotkv.Delete(ctx, s, state.Leaves, leafID[:])
	if err != nil {
		return nil, err
	}
	state.Leaves = *leafState
	return &state, nil
}

// AddGroupLeaf adds a leaf to a group.
func (m *Machine) AddGroupLeaf(ctx context.Context, s stores.RW, State State, kemSeed *[64]byte, groupName string, leafID inet256.ID) (*State, error) {
	group, err := m.GetGroup(ctx, s, State, groupName)
	if err != nil {
		return nil, err
	}
	if len(group.LeafKEMs) >= MaxLeavesPerGroup {
		return nil, fmt.Errorf("group %s has too many leaves (%d) to add another", groupName, len(group.LeafKEMs))
	}
	kemPub, _ := DeriveKEM(*kemSeed)
	if !kemPub.Equal(group.KEM) {
		return nil, fmt.Errorf("group %s has a different KEM public key than the one provided. %v != %v", groupName, kemPub, group.KEM)
	}
	// ensure the leaf exists
	leaf, err := m.GetLeaf(ctx, s, State, leafID)
	if err != nil {
		return nil, err
	}
	group.LeafKEMs[leafID] = encryptSeed(nil, leaf.KEMPublicKey, kemSeed)
	groupState, err := m.gotkv.Mutate(ctx, s, State.Groups, putGroup(*group))
	if err != nil {
		return nil, err
	}
	State.Groups = *groupState
	return &State, nil
}

func (m *Machine) ForEachLeaf(ctx context.Context, s stores.Reading, State State, group string, fn func(leaf IdentityLeaf) error) error {
	span := gotkv.SingleKeySpan([]byte(group))
	return m.gotkv.ForEach(ctx, s, State.Leaves, span, func(ent gotkv.Entry) error {
		if len(ent.Key) != len(group)+32 {
			return nil // potentially for another group.
		}
		leaf, err := ParseIdentityLeaf(ent.Key, ent.Value)
		if err != nil {
			return err
		}
		return fn(*leaf)
	})
}

// PutGroup adds or replaces a group by name.
func (m *Machine) PutGroup(ctx context.Context, s stores.RW, state State, group Group) (*State, error) {
	if strings.ContainsAny(group.Name, "\x00") {
		return nil, fmt.Errorf("group name contains null bytes")
	}
	groupState, err := m.gotkv.Mutate(ctx, s, state.Groups, putGroup(group))
	if err != nil {
		return nil, err
	}
	state.Groups = *groupState
	return &state, nil
}

// GetGroup returns a group by name.
func (m *Machine) GetGroup(ctx context.Context, s stores.Reading, State State, name string) (*Group, error) {
	k := []byte(name)
	val, err := m.gotkv.Get(ctx, s, State.Groups, k)
	if err != nil {
		return nil, err
	}
	return ParseGroup(k, val)
}

// GetKEMSeed returns a KEM seed used to derive the key pair for a given group.
// id is the ID of the leaf that is requesting the KEM private key.
// kemPriv is the KEM private key for the leaf to decrypt messages sent to it by group operations.
// groupPath should go from the largest group to the smallest group.
func (m *Machine) GetKEMSeed(ctx context.Context, s stores.Reading, state State, groupPath []string, id inet256.ID, kemPriv kem.PrivateKey) (*[64]byte, error) {
	var kemSeed *[64]byte
	for len(groupPath) > 0 {
		groupName := groupPath[len(groupPath)-1]
		groupPath = groupPath[:len(groupPath)-1]
		g, err := m.GetGroup(ctx, s, state, groupName)
		if err != nil {
			return nil, err
		}
		if ctext, ok := g.LeafKEMs[id]; ok {
			seed, err := decryptSeed(kemPriv, ctext)
			if err != nil {
				return nil, err
			}
			kemSeed = seed
			_, kemPriv = DeriveKEM(*seed)
		}
	}
	if kemSeed == nil {
		return nil, fmt.Errorf("KEM seed not found")
	}
	return kemSeed, nil
}

// ForEachGroup calls fn for each group in the namespace.
func (m *Machine) ForEachGroup(ctx context.Context, s stores.Reading, State State, fn func(group Group) error) error {
	span := gotkv.TotalSpan()
	return m.gotkv.ForEach(ctx, s, State.Groups, span, func(ent gotkv.Entry) error {
		group, err := ParseGroup(ent.Key, ent.Value)
		if err != nil {
			return err
		}
		return fn(*group)
	})
}

// ForEachMembership calls fn for each membership in the group.
func (m *Machine) ForEachMembership(ctx context.Context, s stores.Reading, State State, group string, fn func(mem Membership) error) error {
	span := gotkv.SingleKeySpan(memberKey(nil, group, ""))
	return m.gotkv.ForEach(ctx, s, State.Memberships, span, func(ent gotkv.Entry) error {
		mem, err := ParseMembership(ent.Key, ent.Value)
		if err != nil {
			return err
		}
		if mem.Group != group {
			return nil
		}
		return fn(*mem)
	})
}

// AddMember adds a member group to a containing group.
func (m *Machine) AddMember(ctx context.Context, s stores.RW, State State, name string, member string) (*State, error) {
	if strings.ContainsAny(name, "\x00") {
		return nil, fmt.Errorf("member name contains null bytes")
	}
	_, err := m.GetGroup(ctx, s, State, name)
	if err != nil {
		return nil, err
	}
	memState, err := m.gotkv.Mutate(ctx, s, State.Memberships,
		putMember(Membership{
			Group:  name,
			Member: member,
		}),
	)
	if err != nil {
		return nil, err
	}
	State.Memberships = *memState
	return &State, nil
}

// RemoveMember removes a member group from a containing group.
func (m *Machine) RemoveMember(ctx context.Context, s stores.RW, State State, group string, member string) (*State, error) {
	// TODO: Need to make sure group exists first.
	memState, err := m.gotkv.Mutate(ctx, s, State.Memberships,
		rmMember(group, member),
	)
	if err != nil {
		return nil, err
	}
	State.Memberships = *memState
	return &State, nil
}

func (m *Machine) GetMembership(ctx context.Context, s stores.Reading, State State, group, mem string) (*Membership, error) {
	k := memberKey(nil, group, mem)
	val, err := m.gotkv.Get(ctx, s, State.Memberships, k)
	if err != nil {
		return nil, err
	}
	return ParseMembership(k, val)
}

// RekeyGroup creates a new KEM key pair for a group and
// - changes the group's KEM public key
// - re-encrypts the KEM private key for all leaves, using the leaves' KEM public key
// - re-encrypts the KEM private key for all member groups, using those groups' KEM public keys
func (m *Machine) RekeyGroup(ctx context.Context, s stores.RW, State State, name string, kemSeed *[64]byte) (*State, error) {
	group, err := m.GetGroup(ctx, s, State, name)
	if err != nil {
		return nil, err
	}

	kemPub, _ := DeriveKEM([64]byte(*kemSeed))
	// update group record
	group.KEM = kemPub
	for leafID := range group.LeafKEMs {
		leaf, err := m.GetLeaf(ctx, s, State, leafID)
		if err != nil {
			return nil, err
		}
		kemCtext := encryptSeed(nil, leaf.KEMPublicKey, kemSeed)
		group.LeafKEMs[leafID] = kemCtext
	}
	groupsRoot, err := m.gotkv.Mutate(ctx, s, State.Groups, putGroup(*group))
	if err != nil {
		return nil, err
	}
	State.Groups = *groupsRoot

	var memMuts []gotkv.Mutation
	if err := m.ForEachMembership(ctx, s, State, name, func(mem Membership) error {
		subgroup, err := m.GetGroup(ctx, s, State, mem.Member)
		if err != nil {
			return err
		}
		mem.EncryptedKEM = encryptSeed(nil, subgroup.KEM, kemSeed)
		memMuts = append(memMuts, putMember(mem))
		return nil
	}); err != nil {
		return nil, err
	}

	memRoot, err := m.gotkv.Mutate(ctx, s, State.Memberships, memMuts...)
	if err != nil {
		return nil, err
	}
	State.Memberships = *memRoot

	return &State, nil
}

// ForEachInGroup calls fn recursively for each ID in the group.
func (m *Machine) ForEachInGroup(ctx context.Context, s stores.Reading, State State, group string, fn func(inet256.ID) error) error {
	return m.ForEachLeaf(ctx, s, State, group, func(leaf IdentityLeaf) error {
		return fn(leaf.ID)
	})
}

// GroupContains returns true if the actor is a member of the group.
func (m *Machine) GroupContains(ctx context.Context, s stores.Reading, State State, group string, actor inet256.ID) (bool, error) {
	var contains bool
	stopEarly := errors.New("stop early")
	if err := m.ForEachInGroup(ctx, s, State, group, func(id inet256.ID) error {
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

type Group struct {
	// Name uniquely identifies the group, it is the primary key of the Groups table.
	Name string

	// KEM is used to send messages to the group.
	// The private key is stored encrypted in each Membership entry.
	KEM kem.PublicKey

	// Leaves are the leaves that are part of the group.
	// The key in the leaves map is the leaf ID.
	// The value in the leaves map is the group's KEM private key encrypted for the leaf to read.
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
	if err := unmarshalIDMap(leavesData, leaves); err != nil {
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
	out = sbe.AppendLP(out, marshalIDMap(nil, g.LeafKEMs))
	out = sbe.AppendLP(out, marshalGroupOwners(nil, g.Owners))
	return out
}

func marshalIDMap(out []byte, leaves map[inet256.ID][]byte) []byte {
	keys := slices.Collect(maps.Keys(leaves))
	slices.SortFunc(keys, compareLeafIDs)
	for _, leafID := range keys {
		leafKEM := leaves[leafID]
		var ent []byte
		ent = append(ent, leafID[:]...)
		ent = append(ent, leafKEM...)

		out = sbe.AppendLP(out, ent)
	}
	return out
}

func unmarshalIDMap(data []byte, dst map[inet256.ID][]byte) error {
	clear(dst)
	var lastID inet256.ID
	for len(data) > 0 {
		ent, rest, err := sbe.ReadLP(data)
		if err != nil {
			return err
		}
		if len(ent) < 32 {
			return fmt.Errorf("map entry cannot be less than 32 bytes. %d", len(ent))
		}
		id := inet256.IDFromBytes(ent[:32])
		if compareLeafIDs(id, lastID) <= 0 {
			return fmt.Errorf("leaves are not sorted")
		}
		// insert into the map
		dst[id] = ent[32:]

		lastID = id
		data = rest
	}
	return nil
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

// IdentityLeaf contains information about a specific signing key.
// It is an entry in the Leaves table.
type IdentityLeaf struct {
	// ID is part of the key.
	ID inet256.ID

	// PublicKey is the public signing key.
	PublicKey inet256.PublicKey
	// KEMPublicKey is the public KEM key.
	// This will have been authenticated by the leaf's.
	KEMPublicKey kem.PublicKey
}

// NewLeaf creates a new IdentityLeaf with a new KEM key pair.
func NewLeaf(pubKey inet256.PublicKey, kemPub kem.PublicKey) IdentityLeaf {
	return IdentityLeaf{
		ID:           pki.NewID(pubKey),
		PublicKey:    pubKey,
		KEMPublicKey: kemPub,
	}
}

func ParseIdentityLeaf(key, value []byte) (*IdentityLeaf, error) {
	id, err := parseLeafKey(key)
	if err != nil {
		return nil, err
	}
	pkData, data, err := sbe.ReadLP(value)
	if err != nil {
		return nil, err
	}
	pubKey, err := pki.ParsePublicKey(pkData)
	if err != nil {
		return nil, err
	}
	kemPubData, _, err := sbe.ReadLP(data)
	if err != nil {
		return nil, err
	}
	kemPub, err := ParseKEMPublicKey(kemPubData)
	if err != nil {
		return nil, err
	}
	return &IdentityLeaf{
		ID:           id,
		PublicKey:    pubKey,
		KEMPublicKey: kemPub,
	}, nil
}

// parseLeafKey parses the key portion of the GotKV entry in the Leaves table.
// The first part of the key is the group name, and the last 32 bytes are the ID.
func parseLeafKey(key []byte) (id inet256.ID, _ error) {
	if len(key) != 32 {
		return inet256.ID{}, fmt.Errorf("leaf key too short")
	}
	return inet256.IDFromBytes(key[:]), nil
}

// Key returns the key portion of the GotKV entry in the Leaves table.
func (il IdentityLeaf) Key(out []byte) []byte {
	return append(out, il.ID[:]...)
}

// Value returns the value portion of the GotKV entry in the Leaves table.
func (il *IdentityLeaf) Value(out []byte) []byte {
	pubKeyData, err := pki.MarshalPublicKey(nil, il.PublicKey)
	if err != nil {
		panic(err)
	}
	out = sbe.AppendLP(out, pubKeyData)
	out = sbe.AppendLP(out, MarshalKEMPublicKey(nil, KEM_MLKEM1024, il.KEMPublicKey))
	return out
}

func (il *IdentityLeaf) GenerateKEM(sigPriv inet256.PrivateKey) kem.PrivateKey {
	pub, priv, err := mlkem1024.GenerateKeyPair(rand.Reader)
	if err != nil {
		panic(err)
	}
	il.KEMPublicKey = pub
	return priv
}

type LeafPrivate struct {
	SigPrivateKey inet256.PrivateKey
	KEMPrivateKey kem.PrivateKey
}

// Membership contains the Group's KEM seed encrypted for the target member.
type Membership struct {
	Group  string
	Member string

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

func memberKey(key []byte, group string, member string) []byte {
	key = append(key, []byte(group)...)
	key = append(key, []byte(member)...)
	key = binary.LittleEndian.AppendUint32(key, uint32(len(member)))
	return key
}

func parseMemberKey(key []byte) (group string, member string, _ error) {
	if len(key) < 4 {
		return "", "", errors.New("key too short")
	}
	memberLen := binary.LittleEndian.Uint32(key[:4])
	group = string(key[0 : len(key)-4])
	member = string(key[4+memberLen:])
	return group, member, nil
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
func rmMember(group string, member string) gotkv.Mutation {
	key := memberKey(nil, group, member)
	return gotkv.Mutation{
		Span:    gotkv.SingleKeySpan(key),
		Entries: []gotkv.Entry{},
	}
}

// putLeaf returns a gotkv mutation that adds a leaf to the leaves table.
func putLeaf(leaf IdentityLeaf) gotkv.Mutation {
	key := leaf.Key(nil)
	return gotkv.Mutation{
		Span: gotkv.SingleKeySpan(key),
		Entries: []gotkv.Entry{
			{Key: key, Value: leaf.Value(nil)},
		},
	}
}

// encryptSeed encryptes a secret seed
func encryptSeed(out []byte, recvKEM kem.PublicKey, secretSeed *[64]byte) []byte {
	kemCtext, ss, err := recvKEM.Scheme().Encapsulate(recvKEM)
	if err != nil {
		panic(err)
	}
	out = append(out, kemCtext...)
	out = appendXOR(out, (*[32]byte)(ss), secretSeed[:])
	return out
}

// decryptSeed decrypts a secret seed
func decryptSeed(recvKEM kem.PrivateKey, ctext []byte) (*[64]byte, error) {
	kemCtextSize := recvKEM.Scheme().CiphertextSize()
	if len(ctext) < kemCtextSize {
		return nil, fmt.Errorf("ctext too short to contain KEM ciphertext")
	}
	kemCtext := ctext[:kemCtextSize]
	ss, err := recvKEM.Scheme().Decapsulate(recvKEM, kemCtext)
	if err != nil {
		return nil, err
	}
	var seed [64]byte
	copy(seed[:], ctext[kemCtextSize:])
	cryptoXOR((*[32]byte)(ss), seed[:], seed[:])
	return &seed, nil
}

func symEncrypt(out []byte, secret *[32]byte, nonce *[12]byte, ptext []byte, ad []byte) []byte {
	cipher, err := chacha20poly1305.New(secret[:])
	if err != nil {
		panic(err)
	}
	return cipher.Seal(out, nonce[:], ptext, ad)
}

func symDecrypt(out []byte, secret *[32]byte, nonce *[12]byte, ctext []byte, ad []byte) ([]byte, error) {
	cipher, err := chacha20poly1305.New(secret[:])
	if err != nil {
		return nil, err
	}
	return cipher.Open(out, nonce[:], ctext, ad)
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
