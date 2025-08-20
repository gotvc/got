package gotns

import (
	"crypto/rand"
	"encoding/binary"
	"errors"

	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/kem/mlkem/mlkem1024"
	"github.com/gotvc/got/src/gotkv"
	"go.inet256.org/inet256/src/inet256"
)

type Group struct {
	Name string
	KEM  kem.PublicKey

	// Owners are the identities that can add and remove members from the group.
	// Owners must also be members of the group.
	Owners []inet256.ID
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
	KEMs      []SignedKEM
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
	il.KEMs = append(il.KEMs, SignedKEM{
		KEM: pub,
		Sig: sig,
	})
	if !il.Verify(sigPriv.Public().(inet256.PublicKey)) {
		panic("this is a bug")
	}
	return priv
}

func (il *IdentityLeaf) Verify(pub inet256.PublicKey) bool {
	for _, kem := range il.KEMs {
		if !kem.Verify(pub) {
			return false
		}
	}
	return true
}

type SignedKEM struct {
	KEM kem.PublicKey
	Sig []byte
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

	GroupKEMs []EncryptedKEM
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
	out = appendLP(out, []byte(tag))
	kemData, err := kem.MarshalBinary()
	if err != nil {
		panic(err)
	}
	out = append(out, kemData...)
	return out
}

// UnmarshalKEMPublicKey unmarshals a KEM public key with a scheme tag.
func UnmarshalKEMPublicKey(data []byte, mk func(tag string) kem.Scheme) (kem.PublicKey, error) {
	tag, n, err := readLP(data)
	if err != nil {
		return nil, err
	}
	data = data[n:]
	scheme := mk(string(tag))
	pubKey, err := scheme.UnmarshalBinaryPublicKey(data)
	if err != nil {
		return nil, err
	}
	return pubKey, nil
}
