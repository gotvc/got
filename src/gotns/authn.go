package gotns

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/kem/mlkem/mlkem1024"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.inet256.org/inet256/src/inet256"
)

func (m *Machine) AddMember(ctx context.Context, s stores.RW, State State, name string, member string) (*State, error) {
	// TODO: Need to make sure group exists first.
	memState, err := m.gotkv.Mutate(ctx, s, State.Memberships,
		addMember(Membership{
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

func (m *Machine) GetGroup(ctx context.Context, s stores.Reading, State State, name string) (*Group, error) {
	k := []byte(name)
	val, err := m.gotkv.Get(ctx, s, State.Groups, k)
	if err != nil {
		return nil, err
	}
	return ParseGroup(k, val)
}

type Group struct {
	Name string
	// KEM is used to send messages to the group.
	// The private key is stored encrypted in each Membership entry.
	KEM kem.PublicKey

	// Owners are the identities that can add and remove members from the group.
	// Owners must also be members of the group.
	Owners []inet256.ID
}

func ParseGroup(key, value []byte) (*Group, error) {
	var group Group
	readLP(value)
	group.Name = string(key)
	return &group, nil
}

func (g *Group) Key(out []byte) []byte {
	out = append(out, []byte(g.Name)...)
	return out
}

func (g *Group) Value(out []byte) []byte {
	out = MarshalKEMPublicKey(out, g.Name, g.KEM)
	return out
}

// IdentityLeaf contains information about a specific signing key.
type IdentityLeaf struct {
	PublicKey inet256.PublicKey
	KEM       kem.PublicKey
	Sig       []byte
}

func ParseIdentityLeaf(data []byte) (*IdentityLeaf, error) {
	pkData, rest, err := readLP(data)
	if err != nil {
		return nil, err
	}
	pubKey, err := inet256.ParsePublicKey(pkData)
	if err != nil {
		return nil, err
	}
	data = rest
	sigKEMData, _, err := readLP(data)
	if err != nil {
		return nil, err
	}
	sigKEM, err := ParseSignedKEM(sigKEMData)
	if err != nil {
		return nil, err
	}
	if !sigKEM.Verify(pubKey) {
		return nil, fmt.Errorf("invalid signature for kem public key")
	}
	return &IdentityLeaf{
		PublicKey: pubKey,
		KEM:       sigKEM.KEM,
		Sig:       sigKEM.Sig,
	}, nil
}

func (il *IdentityLeaf) Marshal(out []byte) []byte {
	pkData, err := il.PublicKey.MarshalBinary()
	if err != nil {
		panic(err)
	}
	out = appendLP(out, pkData)
	out = appendLP(out, il.Sig)
	return out
}

var sigCtxKEM inet256.SigCtx = inet256.SigCtxString("gotns/kem-public-key")

func (il *IdentityLeaf) GenerateKEM(sigPriv inet256.PrivateKey) kem.PrivateKey {
	pub, priv, err := mlkem1024.GenerateKeyPair(rand.Reader)
	if err != nil {
		panic(err)
	}
	kemData, err := pub.MarshalBinary()
	if err != nil {
		panic(err)
	}
	sig := inet256.DefaultPKI.Sign(&sigCtxKEM, sigPriv, kemData, nil)
	if !inet256.DefaultPKI.Verify(&sigCtxKEM, sigPriv.Public().(inet256.PublicKey), kemData, sig) {
		panic("this is a bug")
	}
	il.KEM = pub
	il.Sig = sig
	return priv
}

type SignedKEM struct {
	KEM kem.PublicKey
	Sig []byte
}

// ParseSignedKEM parses a signed KEM from the data.
func ParseSignedKEM(data []byte) (*SignedKEM, error) {
	kemData, rest, err := readLP(data)
	if err != nil {
		return nil, err
	}
	data = rest
	kemPub, err := UnmarshalKEMPublicKey(kemData)
	if err != nil {
		return nil, err
	}
	sig, _, err := readLP(data)
	if err != nil {
		return nil, err
	}
	return &SignedKEM{KEM: kemPub, Sig: sig}, nil
}

func (sk *SignedKEM) Verify(pub inet256.PublicKey) bool {
	kemData, err := sk.KEM.MarshalBinary()
	if err != nil {
		return false
	}
	return inet256.DefaultPKI.Verify(&sigCtxKEM, pub, kemData, sk.Sig)
}

// Membership contains the Group's KEM encrypted for the target member.
type Membership struct {
	Group  string
	Member string

	GroupKEMs [][]byte
}

func ParseMembership(k, v []byte) (*Membership, error) {
	group, member, err := parseMemberKey(k)
	if err != nil {
		return nil, err
	}
	var kems [][]byte
	for len(v) > 0 {
		ctext, rest, err := readLP(v)
		if err != nil {
			return nil, err
		}
		v = rest
		kems = append(kems, ctext)
	}
	return &Membership{
		Group:     group,
		Member:    member,
		GroupKEMs: kems,
	}, nil
}

func (m *Membership) Key(out []byte) []byte {
	return memberKey(out, m.Group, m.Member)
}

func (m *Membership) Value(out []byte) []byte {
	return nil
}

type EncryptedKEM struct {
	KEM kem.PublicKey
}

func memberKey(key []byte, group string, member string) []byte {
	key = append(key, []byte(group)...)
	key = append(key, []byte(member)...)
	key = binary.BigEndian.AppendUint32(key, uint32(len(member)))
	return key
}

func parseMemberKey(key []byte) (group string, member string, _ error) {
	if len(key) < 4 {
		return "", "", errors.New("key too short")
	}
	memberLen := binary.BigEndian.Uint32(key[:4])
	group = string(key[0 : len(key)-4])
	member = string(key[4+memberLen:])
	return group, member, nil
}

// addMember returns a mutation that adds a member to a group.
func addMember(mem Membership) gotkv.Mutation {
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

// MarshalKEMPublicKey marshals a KEM public key with a scheme tag.
func MarshalKEMPublicKey(out []byte, tag string, kem kem.PublicKey) []byte {
	out = appendLP16(out, []byte(tag))
	kemData, err := kem.MarshalBinary()
	if err != nil {
		panic(err)
	}
	out = append(out, kemData...)
	return out
}

// UnmarshalKEMPublicKey unmarshals a KEM public key with a scheme tag.
func UnmarshalKEMPublicKey(data []byte) (kem.PublicKey, error) {
	tag, err := readLP16(data)
	if err != nil {
		return nil, err
	}
	scheme := getKEMScheme(string(tag))
	if scheme == nil {
		return nil, fmt.Errorf("unknown kem scheme: %s", string(tag))
	}
	data = data[2:]
	pubKey, err := scheme.UnmarshalBinaryPublicKey(data)
	if err != nil {
		return nil, err
	}
	return pubKey, nil
}

const (
	KEM_MLKEM1024 = "mlkem1024"
)

func getKEMScheme(tag string) kem.Scheme {
	switch tag {
	case KEM_MLKEM1024:
		return mlkem1024.Scheme()
	default:
		return nil
	}
}
