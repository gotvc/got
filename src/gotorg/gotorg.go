package gotorg

import (
	"context"
	"crypto/rand"
	"fmt"

	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/statetrace"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotorg/internal/gotorgop"
	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/slices2"
	"go.inet256.org/inet256/src/inet256"
)

func Parse(x []byte) (statetrace.Root[Root], error) {
	return statetrace.Parse(x, ParseRoot)
}

type Root struct {
	Version uint8
	// Current is the state of the world.
	Current State
	// Recent is the Delta that was applied most recently
	// to get to the Current state, from the immediately previous state.
	Recent Delta
}

func (r Root) Marshal(out []byte) []byte {
	out = append(out, r.Version)
	out = sbe.AppendLP(out, r.Current.Marshal(nil))
	out = sbe.AppendLP(out, r.Recent.Marshal(nil))
	return out
}

func (r *Root) Unmarshal(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("gotorg: data too short to contain version")
	}
	version, data := data[0], data[1:]
	curData, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	recData, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	current, err := parseState(curData)
	if err != nil {
		return err
	}
	recent, err := parseDelta(recData)
	if err != nil {
		return err
	}
	*r = Root{
		Version: version,
		Current: current,
		Recent:  recent,
	}
	return nil
}

func ParseRoot(data []byte) (Root, error) {
	var root Root
	if err := root.Unmarshal(data); err != nil {
		return Root{}, err
	}
	return root, nil
}

// State represents the current state of a namespace.
type State struct {
	// Authentication tables.

	// IDUnits hold primitive identity elements.
	// The key is the INET256 ID, derived by hashing the public signing key.
	// The value is the public signing key for the unit, and a signed KEM key for the leaf to recieve messages
	// The unit KEM private keys are not store anywhere in the state.
	IDUnits gotkv.Root
	// Groups maps group IDs to group information.
	// Group information holds group's Owner list, and shared KEM key for the group
	// to receive messages.
	Groups gotkv.Root
	// GroupNames maps names to group IDs.
	GroupNames gotkv.Root

	// Memberships holds relationships between groups and other groups.
	// The value is a KEM ciphertext sent by the containing group owners to the member.
	// The ciphertext contains the containing group's KEM private key encrypted with the member's KEM public key.
	Memberships gotkv.Root

	// Authorization tables.

	// Rules holds rules for the namespace, granting look or touch access to marks.
	Rules gotkv.Root
	// Obligations holds obligations for the namespace, granting access to volumes.
	Obligations gotkv.Root

	// Content tables

	// Volumes holds the volume entries themselves.
	Volumes gotkv.Root
	// VolumeNames maps names to Volume OIDs
	VolumeNames gotkv.Root
}

func (s State) Marshal(out []byte) []byte {
	const versionTag = 0
	out = append(out, versionTag)

	out = sbe.AppendLP(out, s.IDUnits.Marshal(nil))
	out = sbe.AppendLP(out, s.Groups.Marshal(nil))
	out = sbe.AppendLP(out, s.GroupNames.Marshal(nil))
	out = sbe.AppendLP(out, s.Memberships.Marshal(nil))

	out = sbe.AppendLP(out, s.Rules.Marshal(nil))
	out = sbe.AppendLP(out, s.Obligations.Marshal(nil))

	out = sbe.AppendLP(out, s.Volumes.Marshal(nil))
	out = sbe.AppendLP(out, s.VolumeNames.Marshal(nil))
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
	for _, dst := range []*gotkv.Root{
		&s.IDUnits, &s.Groups, &s.GroupNames, &s.Memberships,

		&s.Rules, &s.Obligations,

		&s.Volumes, &s.VolumeNames,
	} {
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
type Delta gotorgop.ChangeSet

func parseDelta(data []byte) (Delta, error) {
	var cs gotorgop.ChangeSet
	if err := cs.Unmarshal(data); err != nil {
		return Delta{}, err
	}
	return Delta(cs), nil
}

func (d Delta) Marshal(out []byte) []byte {
	return gotorgop.ChangeSet(d).Marshal(out)
}

type Machine struct {
	gotkv gotkv.Machine
	led   statetrace.Machine[Root]
}

func New() Machine {
	m := Machine{
		gotkv: gotkv.NewMachine(1<<14, 1<<20),
		led: statetrace.Machine[Root]{
			ParseState: ParseRoot,
		},
	}
	m.led.Verify = func(ctx context.Context, s schema.RO, prev, next Root) error {
		return m.ValidateChange(ctx, s, prev.Current, next.Current, next.Recent)
	}
	return m
}

// New creates a new root.
func (m *Machine) New(ctx context.Context, s stores.RW, admins []IdentityUnit) (*statetrace.Root[Root], error) {
	const adminGroupName = "admin"
	state := new(State)
	for _, dst := range []*gotkv.Root{
		&state.IDUnits,

		&state.Groups,
		&state.GroupNames,
		&state.Memberships,

		&state.Rules, &state.Obligations,

		&state.Volumes, &state.VolumeNames,
	} {
		kvr, err := m.gotkv.NewEmpty(ctx, s)
		if err != nil {
			return nil, err
		}
		*dst = *kvr
	}

	// create initial group secret
	var groupSecret gotorgop.Secret
	if _, err := rand.Read(groupSecret[:]); err != nil {
		return nil, err
	}
	groupKEMPub, _ := groupSecret.DeriveKEM()
	units := map[inet256.ID][]byte{}
	for _, unit := range admins {
		var err error
		state, err = m.PutIDUnit(ctx, s, *state, unit)
		if err != nil {
			return nil, err
		}
		units[unit.ID] = encryptSeed(nil, unit.KEMPublicKey, &groupSecret)
	}

	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	g := gotorgop.Group{
		Nonce:  nonce,
		KEM:    groupKEMPub,
		Owners: slices2.Map(admins, func(leaf IdentityUnit) inet256.ID { return leaf.ID }),
	}
	g.ID = gotorgop.ComputeGroupID(g.Nonce, g.Owners)
	var err error
	state, err = m.PutGroup(ctx, s, *state, g)
	if err != nil {
		return nil, err
	}
	if state, err = m.PutGroupName(ctx, s, *state, adminGroupName, g.ID); err != nil {
		return nil, err
	}
	for unit := range units {
		mem := MemberUnit(unit)
		state, err = m.AddMember(ctx, s, *state, g.ID, mem, &groupSecret)
		if err != nil {
			return nil, err
		}
	}

	state, err = m.addInitialRules(ctx, s, *state, g.ID)
	if err != nil {
		return nil, err
	}

	if err := m.ValidateState(ctx, s, *state); err != nil {
		panic(err)
	}
	root := Root{
		Current: *state,
	}
	root2 := m.led.Initial(root)
	return &root2, nil
}

// ValidateState checks the state in isolation.
func (m *Machine) ValidateState(ctx context.Context, src stores.Reading, x State) error {
	for i, kvr := range []gotkv.Root{
		x.IDUnits,
		x.Groups, x.GroupNames, x.Memberships,

		x.Rules, x.Obligations,

		x.VolumeNames, x.Volumes,
	} {
		if kvr.Ref.CID.IsZero() {
			return fmt.Errorf("gotorg: one of the States is uninitialized %d", i)
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

// validateOp validates an operation in isolation.
func (m *Machine) validateOp(ctx context.Context, src stores.Reading, prev, next State, approvers func(inet256.ID) bool, op Op) error {
	switch op := op.(type) {
	case *gotorgop.ChangeSet:
		return m.validateChangeSet(ctx, src, prev, next, approvers, op)
	default:
		return fmt.Errorf("unsupported op: %T", op)
	}
}

func (m *Machine) validateChangeSet(ctx context.Context, src stores.Reading, prev, next State, approvers func(inet256.ID) bool, op *gotorgop.ChangeSet) error {
	for _, op2 := range op.Ops {
		if err := m.validateOp(ctx, src, prev, next, approvers, op2); err != nil {
			return err
		}
	}
	return nil
}

func (m *Machine) mapKV(ctx context.Context, s stores.RW, kvr *gotkv.Root, fn func(tx *gotkv.Tx) error) error {
	tx := m.gotkv.NewTx(s, *kvr)
	if err := fn(tx); err != nil {
		return err
	}
	nextKvr, err := tx.Flush(ctx)
	if err != nil {
		return err
	}
	*kvr = *nextKvr
	return nil
}

// putGroup returns a gotkv mutation that puts a group into the groups table.
func putGroup(group gotorgop.Group) gotkv.Edit {
	return gotkv.Edit{
		Span: gotkv.SingleKeySpan(group.Key(nil)),
		Entries: []gotkv.Entry{
			{
				Key:   group.Key(nil),
				Value: group.Value(nil),
			},
		},
	}
}

type ChangeSet = gotorgop.ChangeSet
