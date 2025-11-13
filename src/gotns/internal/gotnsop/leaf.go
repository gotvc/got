package gotnsop

import (
	"crypto/rand"
	"fmt"

	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/kem/mlkem/mlkem1024"
	"github.com/gotvc/got/src/internal/sbe"
	"go.inet256.org/inet256/src/inet256"
)

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
