package gotns

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/gotled"
	"github.com/gotvc/got/src/internal/stores"
)

// Root contains references to the entire state of the namespace, including the history.
type Root = gotled.Root[State, Delta]

func ParseRoot(data []byte) (Root, error) {
	return gotled.Parse(data, parseState, parseDelta)
}

// State represents the current state of a namespace.
type State struct {
	// Groups maps group names to group information.
	// Group information holds group's Owner list, and shared KEM key for the group
	// to receive messages.
	Groups gotkv.Root
	// Leaves hold relationships between groups and primitive identity elements.
	// The first part of the key is the group name, and the last 32 bytes are the Leaf's ID.
	// The value is the public signing key for the leaf, and a signed KEM key for the leaf
	// to recieve messages
	Leaves gotkv.Root
	// Memberships holds relationships between groups and other groups.
	// The key is the containing group + the member group + len(member group name)
	// The value is a KEM ciphertext sent by the containing group owner to the member.
	// The ciphertext contains the containing group's KEM private key encrypted with the member's KEM public key.
	Memberships gotkv.Root

	// Rules holds rules for the namespace, granting look or touch access to branches.
	Rules gotkv.Root

	// Branches holds the actual branch entries themselves.
	// Branch entries contain a volume OID, and a set of rights (which is for Blobcache)
	// They also contain a set of DEK hashes for the DEKs that should be used to encrypt the volume
	// and a map from Hash(KEM public key) to the KEM ciphertext for the DEK encrypted with the KEM public key.
	Branches gotkv.Root
}

func (s State) Marshal(out []byte) []byte {
	const versionTag = 0
	out = append(out, versionTag)
	out = appendLP(out, s.Groups.Marshal())
	out = appendLP(out, s.Leaves.Marshal())
	out = appendLP(out, s.Memberships.Marshal())
	out = appendLP(out, s.Rules.Marshal())
	out = appendLP(out, s.Branches.Marshal())
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
	for _, dst := range []*gotkv.Root{&s.Groups, &s.Leaves, &s.Memberships, &s.Rules, &s.Branches} {
		kvrData, rest, err := readLP(data)
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
type Delta []Op

func parseDelta(data []byte) (Delta, error) {
	var ret Delta
	for len(data) > 0 {
		op, rest, err := ReadOp(data)
		if err != nil {
			return Delta{}, err
		}
		data = rest
		ret = append(ret, op)
	}
	return ret, nil
}

func (d Delta) Marshal(out []byte) []byte {
	for _, op := range d {
		out = AppendOp(out, op)
	}
	return out
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
			ParseDelta: parseDelta,
		},
	}
	m.led.Apply = m.Apply
	return m
}

// New creates a new root.
func (m *Machine) New(ctx context.Context, s stores.RW) (*Root, error) {
	var state State
	for _, dst := range []*gotkv.Root{&state.Groups, &state.Leaves, &state.Memberships, &state.Rules, &state.Branches} {
		kvr, err := m.gotkv.NewEmpty(ctx, s)
		if err != nil {
			return nil, err
		}
		*dst = *kvr
	}
	if err := m.ValidateState(ctx, s, state); err != nil {
		panic(err)
	}
	root := m.led.Initial(state)
	if err := m.ValidateChange(ctx, s, root.State, root.State, Delta{}); err != nil {
		return nil, err
	}
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
func (m *Machine) ValidateChange(ctx context.Context, src stores.Reading, prev, next State, delta Delta) error {
	// TODO: first validate auth operations, ensure that all the differences are signed.
	return nil
}

func (m *Machine) Apply(ctx context.Context, s stores.RW, prev State, delta Delta) (State, error) {
	state := prev
	for _, op := range delta {
		var err error
		state, err = op.perform(ctx, m, s, state)
		if err != nil {
			return State{}, err
		}
	}
	return state, nil
}

// appendLP16 appends a length-prefixed byte slice to out.
// the length is encoded as a 16-bit big-endian integer.
func appendLP16(out []byte, data []byte) []byte {
	out = binary.BigEndian.AppendUint16(out, uint16(len(data)))
	return append(out, data...)
}

// readLP16 reads a length-prefixed byte slice from data.
// the length is encoded as a 16-bit big-endian integer.
func readLP16(data []byte) ([]byte, error) {
	dataLen := binary.BigEndian.Uint16(data)
	if len(data) < 2+int(dataLen) {
		return nil, fmt.Errorf("length-prefixed data too short")
	}
	return data[2 : 2+int(dataLen)], nil
}

// appendLP appends a length-prefixed byte slice to out.
// the length is encoded as a varint.
func appendLP(out []byte, data []byte) []byte {
	out = binary.AppendUvarint(out, uint64(len(data)))
	return append(out, data...)
}

// readLP reads a length-prefixed byte slice from data.
// the length is encoded as a varint.
func readLP(data []byte) (y []byte, rest []byte, _ error) {
	dataLen, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, nil, fmt.Errorf("invalid length-prefixed data")
	}
	if len(data) < n+int(dataLen) {
		return nil, nil, fmt.Errorf("length-prefixed data too short")
	}
	return data[n : n+int(dataLen)], data[n+int(dataLen):], nil
}
