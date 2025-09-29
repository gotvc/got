package gotns

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/gotled"
	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/slices2"
	"go.inet256.org/inet256/src/inet256"
)

// Root contains references to the entire state of the namespace, including the history.
type Root = gotled.Root[State, Delta]

func ParseRoot(data []byte) (Root, error) {
	return gotled.Parse(data, parseState, parseDelta)
}

// State represents the current state of a namespace.
type State struct {
	// Authentication tables.

	// Leaves hold primitive identity elements.
	// The key is the leaf ID, derived by hashing the public signing key.
	// The value is the public signing key for the leaf, and a signed KEM key for the leaf to recieve messages
	// The leaf KEM private keys are not store anywhere in the state.
	Leaves gotkv.Root
	// Groups maps group names to group information.
	// Group information holds group's Owner list, and shared KEM key for the group
	// to receive messages.
	Groups gotkv.Root
	// Memberships holds relationships between groups and other groups.
	// The key is the containing group + the member group + len(member group name)
	// The value is a KEM ciphertext sent by the containing group owner to the member.
	// The ciphertext contains the containing group's KEM private key encrypted with the member's KEM public key.
	Memberships gotkv.Root

	// Authorization tables.

	// Rules holds rules for the namespace, granting look or touch access to branches.
	Rules gotkv.Root

	// Obligations holds obligations for the namespace, granting access to volumes.
	Obligations gotkv.Root

	// Content tables

	// Branches holds the actual branch entries themselves.
	// Branch entries contain a volume OID, and a set of rights (which is for Blobcache)
	// They also contain a set of DEK hashes for the DEKs that should be used to encrypt the volume
	// and a map from Hash(KEM public key) to the KEM ciphertext for the DEK encrypted with the KEM public key.
	Branches gotkv.Root
}

func (s State) Marshal(out []byte) []byte {
	const versionTag = 0
	out = append(out, versionTag)
	out = sbe.AppendLP(out, s.Leaves.Marshal(nil))
	out = sbe.AppendLP(out, s.Groups.Marshal(nil))
	out = sbe.AppendLP(out, s.Memberships.Marshal(nil))
	out = sbe.AppendLP(out, s.Rules.Marshal(nil))
	out = sbe.AppendLP(out, s.Obligations.Marshal(nil))
	out = sbe.AppendLP(out, s.Branches.Marshal(nil))
	return out
}

func (s *State) Unmarshal(data []byte) error {
	// read version tag
	if len(data) < 1 {
		return fmt.Errorf("too short to contain version tag")
	}
	switch data[0] {
	case 0:
	default:
		return fmt.Errorf("unknown version tag: %d", data[0])
	}
	data = data[1:]

	// read all of the gotkv roots
	for _, dst := range []*gotkv.Root{&s.Leaves, &s.Groups, &s.Memberships, &s.Rules, &s.Obligations, &s.Branches} {
		kvrData, rest, err := sbe.ReadLP(data)
		if err != nil {
			return err
		}
		if err := dst.Unmarshal(kvrData); err != nil {
			return err
		}
		data = rest
	}
	return nil
}

func parseState(x []byte) (State, error) {
	if len(x) == 0 {
		return State{}, nil
	}
	var state State
	if err := state.Unmarshal(x); err != nil {
		return State{}, err
	}
	return state, nil
}

// Delta can be applied to a State to produce a new State.
type Delta Op_ChangeSet

func parseDelta(data []byte) (Delta, error) {
	var cs Op_ChangeSet
	if err := cs.Unmarshal(data); err != nil {
		return Delta{}, err
	}
	return Delta(cs), nil
}

func (d Delta) Marshal(out []byte) []byte {
	return Op_ChangeSet(d).Marshal(out)
}

type Machine struct {
	gotkv gotkv.Machine
	led   gotled.Machine[State, Delta]
}

func New() Machine {
	m := Machine{
		gotkv: gotkv.NewMachine(1<<14, 1<<20),
		led: gotled.Machine[State, Delta]{
			ParseState: parseState,
			ParseProof: parseDelta,
		},
	}
	m.led.Verify = m.ValidateChange
	return m
}

// New creates a new root.
func (m *Machine) New(ctx context.Context, s stores.RW, admins []IdentityLeaf) (*Root, error) {
	state := new(State)
	for _, dst := range []*gotkv.Root{&state.Groups, &state.Leaves, &state.Memberships, &state.Rules, &state.Obligations, &state.Branches} {
		kvr, err := m.gotkv.NewEmpty(ctx, s)
		if err != nil {
			return nil, err
		}
		*dst = *kvr
	}

	// create initial KEM seed
	var kemSeed [64]byte
	if _, err := rand.Read(kemSeed[:]); err != nil {
		return nil, err
	}
	groupKEMPub, _ := DeriveKEM(kemSeed)

	leaves := map[inet256.ID][]byte{}
	for _, leaf := range admins {
		var err error
		state, err = m.PutLeaf(ctx, s, *state, leaf)
		if err != nil {
			return nil, err
		}
		leaves[leaf.ID] = encryptSeed(nil, leaf.KEMPublicKey, &kemSeed)
	}
	const adminGroupName = "admin"
	g := Group{
		Name:     adminGroupName,
		KEM:      groupKEMPub,
		LeafKEMs: leaves,
		Owners:   slices2.Map(admins, func(leaf IdentityLeaf) inet256.ID { return leaf.ID }),
	}
	var err error
	state, err = m.PutGroup(ctx, s, *state, g)
	if err != nil {
		return nil, err
	}
	state, err = m.addInitialRules(ctx, s, *state, adminGroupName)
	if err != nil {
		return nil, err
	}

	if err := m.ValidateState(ctx, s, *state); err != nil {
		panic(err)
	}
	root := m.led.Initial(*state)
	return &root, nil
}

// ValidateState checks the state in isolation.
func (m *Machine) ValidateState(ctx context.Context, src stores.Reading, x State) error {
	for _, kvr := range []gotkv.Root{x.Leaves, x.Groups, x.Memberships, x.Rules, x.Branches} {
		if kvr.Ref.CID.IsZero() {
			return fmt.Errorf("gotns: one of the States is uninitialized")
		}
	}
	return nil
}

// ValidateChange checks the change between two states.
// Prev is assumed to be a known good, valid state.
func (m *Machine) ValidateChange(ctx context.Context, src stores.Reading, prev, next State, proof Delta) error {
	if err := m.ValidateState(ctx, src, next); err != nil {
		return err
	}
	// TODO: first validate auth operations, ensure that all the differences are signed.
	return nil
}

// putGroup returns a gotkv mutation that puts a group into the groups table.
func putGroup(group Group) gotkv.Mutation {
	return gotkv.Mutation{
		Span: gotkv.SingleKeySpan(group.Key(nil)),
		Entries: []gotkv.Entry{
			{
				Key:   group.Key(nil),
				Value: group.Value(nil),
			},
		},
	}
}
