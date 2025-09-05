package gotns

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/gotvc/got/src/internal/stores"
	"go.inet256.org/inet256/src/inet256"
)

// Txn allows a transction to be built up incrementally
// And then turned into a single slot change to the ledger.
type Txn struct {
	m       *Machine
	prev    Root
	s       stores.RW
	signers []inet256.PrivateKey

	changes []Op
}

// NewBuilder creates a new delta builder.
// privateKey is the private key of the actor performing the transaction.
// It will be used to produce a signature at the end of the transaction.
func (m *Machine) NewTxn(prev Root, s stores.RW, signers []inet256.PrivateKey) *Txn {
	return &Txn{
		m:       m,
		prev:    prev,
		s:       s,
		signers: signers,
		changes: []Op{},
	}
}

func (b *Txn) AddOp(op Op) {
	b.changes = append(b.changes, op)
}

// Finish applies the changes to the previous root, and returns the new root.
func (tx *Txn) Finish(ctx context.Context) (Root, error) {
	cs := Op_ChangeSet{
		Ops: tx.changes,
	}
	for _, signer := range tx.signers {
		cs.Sign(signer)
	}

	next, err := cs.perform(ctx, tx.m, tx.s, tx.prev.State, IDSet{})
	if err != nil {
		return Root{}, err
	}
	return tx.m.led.AndThen(ctx, tx.s, tx.prev, Delta(cs), next)
}

// CreateLeaf creates a new leaf.
func (tx *Txn) CreateLeaf(ctx context.Context, leaf IdentityLeaf) error {
	tx.AddOp(&Op_CreateLeaf{
		Leaf: leaf,
	})
	return nil
}

// AddLeaf adds a leaf in a transaction.
func (tx *Txn) AddLeaf(ctx context.Context, group string, leafID inet256.ID) error {
	tx.AddOp(&Op_AddMember{
		Group:  group,
		Member: leafID.String(),
	})
	return nil
}

func (tx *Txn) ChangeSet(cs Op_ChangeSet) error {
	tx.AddOp(&cs)
	return nil
}

type OpCode uint8

const (
	OpCode_UNKNOWN OpCode = iota

	OpCode_ChangeSet

	OpCode_CreateGroup
	OpCode_CreateLeaf
	OpCode_AddLeaf
	OpCode_RemoveLeaf
	OpCode_AddMember
	OpCode_RemoveMember

	OpCode_AddRule
	OpCode_DropRule

	OpCode_PutEntry
	OpCode_DeleteEntry
)

type OpHeader [4]byte

func NewOpHeader(code OpCode, payloadLen int) (ret OpHeader) {
	if payloadLen < 0 || payloadLen > 0x00ffffff {
		panic(fmt.Errorf("payload length out of range: %d", payloadLen))
	}
	h := uint32(code)<<24 | uint32(payloadLen)&0x00ffffff
	binary.LittleEndian.PutUint32(ret[:], h)
	return ret
}

func (h OpHeader) Code() OpCode {
	return OpCode(binary.LittleEndian.Uint32(h[:]) >> 24)
}

func (h OpHeader) PayloadLen() int {
	return int(binary.LittleEndian.Uint32(h[:]) & 0x00ffffff)
}

func readHeader(data []byte) (OpHeader, []byte, error) {
	if len(data) < 4 {
		return OpHeader{}, nil, fmt.Errorf("too short to contain op header")
	}
	var header OpHeader
	copy(header[:], data)
	return header, data[4:], nil
}

type IDSet = map[inet256.ID]struct{}

// Op is a single operation on the ledger.
// Ops are batched into Deltas, which represent an atomic state transition of the ledger.
type Op interface {
	// Marshal marshales the op body.  The header is not included.
	Marshal(out []byte) []byte
	// Unmarshal unmarshals the op body, which does not include the header.
	Unmarshal(data []byte) error
	// OpCode returns the op code.
	OpCode() OpCode

	// perform applies the op to the state.
	perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error)

	isOp()
}

// AppendOp appends the op to the output.
// It calls marshal on the op, and prefixes it with the op header.
func AppendOp(out []byte, op Op) []byte {
	payload := op.Marshal(nil)
	header := NewOpHeader(op.OpCode(), len(payload))
	out = append(out, header[:]...)
	out = append(out, payload...)
	return out
}

// ReadOp reads an op from the data.
// It starts by reading the 4 byte OpHeader, and then uses that to parse the rest of the op.
// It returns the op, and the rest of the data, not part of the op
func ReadOp(data []byte) (Op, []byte, error) {
	header, data, err := readHeader(data)
	if err != nil {
		return nil, nil, err
	}
	payload := data[:header.PayloadLen()]
	op, err := parseOp(header.Code(), payload)
	if err != nil {
		return nil, nil, err
	}
	return op, data[header.PayloadLen():], nil
}

func parseOp(code OpCode, payload []byte) (Op, error) {
	var op Op
	switch code {
	case OpCode_ChangeSet:
		op = &Op_ChangeSet{}
	case OpCode_CreateGroup:
		op = &Op_CreateGroup{}
	case OpCode_CreateLeaf:
		op = &Op_CreateLeaf{}
	case OpCode_AddLeaf:
		op = &Op_AddLeaf{}
	case OpCode_RemoveLeaf:
		op = &Op_RemoveLeaf{}
	case OpCode_AddMember:
		op = &Op_AddMember{}
	case OpCode_RemoveMember:
		op = &Op_RemoveMember{}
	case OpCode_AddRule:
		op = &Op_AddRule{}
	case OpCode_DropRule:
		op = &Op_DropRule{}

	case OpCode_PutEntry:
		op = &Op_PutEntry{}
	case OpCode_DeleteEntry:
		op = &Op_DeleteEntry{}
	default:
		return nil, fmt.Errorf("unrecognized op code: %d", code)
	}
	return op, op.Unmarshal(payload)
}

// Op_ChangeSet is a set of operations signed together.
type Op_ChangeSet struct {
	Ops  []Op
	Sigs map[inet256.ID][]byte
}

func (op Op_ChangeSet) isOp() {}

func (op Op_ChangeSet) OpCode() OpCode {
	return OpCode_ChangeSet
}

// OpData is the portion of the change set that contains the operations.
// This is what is signed.
// The other portion is the sig map.
func (op Op_ChangeSet) OpData(out []byte) []byte {
	out = binary.AppendUvarint(out, uint64(len(op.Ops)))
	for _, op := range op.Ops {
		out = AppendOp(out, op)
	}
	return out
}

func (op Op_ChangeSet) Marshal(out []byte) []byte {
	out = op.OpData(out)
	out = marshalIDMap(out, op.Sigs)
	return out
}

func (cs *Op_ChangeSet) Unmarshal(data []byte) error {
	// read ops from the beginning.
	opsLen, n := binary.Uvarint(data)
	if n <= 0 {
		return fmt.Errorf("invalid ops length")
	}
	data = data[n:]
	var ops []Op
	for i := 0; i < int(opsLen); i++ {
		op, rest, err := ReadOp(data)
		if err != nil {
			return err
		}
		ops = append(ops, op)
		data = rest
	}
	// assume the rest of the data is sigs.
	sigs := make(map[inet256.ID][]byte)
	if err := unmarshalIDMap(data, sigs); err != nil {
		return err
	}
	cs.Ops = ops
	cs.Sigs = sigs
	return nil
}

func (cs Op_ChangeSet) perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error) {
	// collect all of the public keys that we need.
	pubKeys := make(map[inet256.ID]inet256.PublicKey)
	// Any create operations will provide public keys that are not yet in the state.
	for _, op := range cs.Ops {
		switch op := op.(type) {
		case *Op_CreateLeaf:
			pubKeys[op.Leaf.ID] = op.Leaf.PublicKey
		}
	}
	for id := range cs.Sigs {
		if _, ok := pubKeys[id]; !ok {
			leaf, err := m.GetLeaf(ctx, s, state, id)
			if err != nil {
				return State{}, err
			}
			pubKeys[id] = leaf.PublicKey
		}
	}
	// validate the sigs.
	target := cs.OpData(nil)
	for id, sig := range cs.Sigs {
		pubKey := pubKeys[id]
		if !inet256.DefaultPKI.Verify(&sigCtxTxn, pubKey, target, sig) {
			return State{}, fmt.Errorf("invalid signature for %v", id)
		}
		approvers[id] = struct{}{}
	}
	// apply the ops.
	for _, op := range cs.Ops {
		var err error
		state, err = op.perform(ctx, m, s, state, approvers)
		if err != nil {
			return State{}, err
		}
	}
	return state, nil
}

var sigCtxTxn = inet256.SigCtxString("gotns/txn")

// Sign signs the change set with the private key and adds the signature to the sigs map.
func (op *Op_ChangeSet) Sign(pk inet256.PrivateKey) {
	pki := inet256.DefaultPKI
	id := pki.NewID(pk.Public().(inet256.PublicKey))
	data := op.OpData(nil)
	sig := pki.Sign(&sigCtxTxn, pk, data, nil)

	if op.Sigs == nil {
		op.Sigs = make(map[inet256.ID][]byte)
	}
	op.Sigs[id] = sig
}

// Op_CreateGroup creates a group.
// It has to be
type Op_CreateGroup struct {
	Group Group
}

func (op Op_CreateGroup) isOp() {}

func (op Op_CreateGroup) Marshal(out []byte) []byte {
	out = appendLP(out, op.Group.Key(nil))
	out = appendLP(out, op.Group.Value(nil))
	return out
}

func (op *Op_CreateGroup) Unmarshal(data []byte) error {
	k, data, err := readLP(data)
	if err != nil {
		return err
	}
	val, _, err := readLP(data)
	if err != nil {
		return err
	}
	group, err := ParseGroup(k, val)
	if err != nil {
		return err
	}
	op.Group = *group
	return nil
}

func (op Op_CreateGroup) OpCode() OpCode {
	return OpCode_CreateGroup
}

func (op Op_CreateGroup) perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error) {
	_, err := m.GetGroup(ctx, s, state, op.Group.Name)
	if err == nil {
		return State{}, fmt.Errorf("group already exists")
	}
	next, err := m.PutGroup(ctx, s, state, op.Group)
	if err != nil {
		return State{}, err
	}
	return *next, nil
}

// Op_CreateLeaf creates a new leaf, or fails if the leaf already exists.
type Op_CreateLeaf struct {
	Leaf IdentityLeaf
}

func (op Op_CreateLeaf) isOp() {}

func (op Op_CreateLeaf) OpCode() OpCode {
	return OpCode_CreateLeaf
}

func (op Op_CreateLeaf) Marshal(out []byte) []byte {
	out = appendLP(out, op.Leaf.Key(nil))
	out = appendLP(out, op.Leaf.Value(nil))
	return out
}

func (op *Op_CreateLeaf) Unmarshal(data []byte) error {
	k, data, err := readLP(data)
	if err != nil {
		return err
	}
	val, _, err := readLP(data)
	if err != nil {
		return err
	}
	leaf, err := ParseIdentityLeaf(k, val)
	if err != nil {
		return err
	}
	op.Leaf = *leaf
	return nil
}

func (op Op_CreateLeaf) perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error) {
	if _, ok := approvers[op.Leaf.ID]; !ok {
		return State{}, fmt.Errorf("cannot create leaf without approval from %v", op.Leaf.ID)
	}
	if _, err := m.GetLeaf(ctx, s, state, op.Leaf.ID); err == nil {
		return State{}, fmt.Errorf("leaf already exists")
	}
	next, err := m.PutLeaf(ctx, s, state, op.Leaf)
	if err != nil {
		return State{}, err
	}
	return *next, nil
}

// Op_AddLeaf adds a leaf to a group.
// The leaf must exist.
// It can be created by Op_CreateLeaf included in the same transaction.
type Op_AddLeaf struct {
	Group  string
	LeafID inet256.ID
}

func (op Op_AddLeaf) isOp() {}

func (op Op_AddLeaf) Marshal(out []byte) []byte {
	out = append(out, op.Group...)
	out = append(out, op.LeafID[:]...)
	return out
}

func (op *Op_AddLeaf) Unmarshal(data []byte) error {
	if len(data) < 32 {
		return fmt.Errorf("invalid leaf id length: %d", len(data))
	}
	group := string(data[:len(data)-32])
	id := inet256.IDFromBytes(data[len(data)-32:])
	op.Group = group
	op.LeafID = id
	return nil
}

func (op Op_AddLeaf) OpCode() OpCode {
	return OpCode_AddLeaf
}

func (op Op_AddLeaf) perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error) {
	_, err := m.GetLeaf(ctx, s, state, op.LeafID)
	if err != nil {
		return State{}, err
	}
	next, err := m.AddGroupLeaf(ctx, s, state, op.Group, op.LeafID)
	if err != nil {
		return State{}, err
	}
	return *next, nil
}

// Op_RemoveLeaf removes a leaf from a group.
// If the leaf is not in any group, it is removed from the leaves table.
type Op_RemoveLeaf struct {
	Group string
	ID    inet256.ID
}

func (op Op_RemoveLeaf) isOp() {}

func (op Op_RemoveLeaf) Marshal(out []byte) []byte {
	out = append(out, op.Group...)
	out = append(out, op.ID[:]...)
	return out
}

func (op *Op_RemoveLeaf) Unmarshal(data []byte) error {
	if len(data) < 32 {
		return fmt.Errorf("invalid leaf id length: %d", len(data))
	}
	group := string(data[:len(data)-32])
	id, err := parseLeafKey(data[len(data)-32:])
	if err != nil {
		return err
	}
	op.Group = group
	op.ID = id
	return nil
}

func (op Op_RemoveLeaf) OpCode() OpCode {
	return OpCode_RemoveLeaf
}

func (op Op_RemoveLeaf) perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error) {
	next, err := m.DropLeaf(ctx, s, state, op.ID)
	if err != nil {
		return State{}, err
	}
	return *next, nil
}

type Op_AddMember struct {
	Group, Member string
	EncryptedKEM  []byte
}

func (op Op_AddMember) isOp() {}

func (op Op_AddMember) Marshal(out []byte) []byte {
	out = appendLP(out, []byte(op.Group))
	out = appendLP(out, []byte(op.Member))
	out = appendLP(out, op.EncryptedKEM)
	return out
}

func (op *Op_AddMember) Unmarshal(data []byte) error {
	group, data, err := readLP(data)
	if err != nil {
		return err
	}
	member, data, err := readLP(data)
	if err != nil {
		return err
	}
	encryptedKEM, _, err := readLP(data)
	if err != nil {
		return err
	}
	op.Group = string(group)
	op.Member = string(member)
	op.EncryptedKEM = encryptedKEM
	return nil
}

func (op Op_AddMember) OpCode() OpCode {
	return OpCode_AddMember
}

func (op Op_AddMember) perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error) {
	_, err := m.GetGroup(ctx, s, state, op.Group)
	if err != nil {
		return State{}, err
	}
	next, err := m.AddMember(ctx, s, state, op.Group, op.Member)
	if err != nil {
		return State{}, err
	}
	return *next, nil
}

type Op_RemoveMember struct {
	Group, Member string
}

func (op Op_RemoveMember) isOp() {}

func (op Op_RemoveMember) Marshal(out []byte) []byte {
	out = appendLP(out, []byte(op.Group))
	out = appendLP(out, []byte(op.Member))
	return out
}

func (op *Op_RemoveMember) Unmarshal(data []byte) error {
	group, data, err := readLP(data)
	if err != nil {
		return err
	}
	member, _, err := readLP(data)
	if err != nil {
		return err
	}
	op.Group = string(group)
	op.Member = string(member)
	return nil
}

func (op Op_RemoveMember) OpCode() OpCode {
	return OpCode_RemoveMember
}

func (op Op_RemoveMember) perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error) {
	panic("not implemented")
}

type Op_AddRule struct {
	Rule Rule
}

func (op Op_AddRule) isOp() {}

func (op Op_AddRule) Marshal(out []byte) []byte {
	out = op.Rule.Marshal(out)
	return out
}

func (op *Op_AddRule) Unmarshal(data []byte) error {
	return op.Rule.Unmarshal(data)
}

func (op Op_AddRule) OpCode() OpCode {
	return OpCode_AddRule
}

func (op Op_AddRule) perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error) {
	yes, err := m.CanAnyDo(ctx, s, state, approvers, "ADMIN", op.Rule.ObjectType, op.Rule.Names.String())
	if err != nil {
		return State{}, err
	}
	if !yes {
		return State{}, fmt.Errorf("cannot add rule")
	}
	next, err := m.AddRule(ctx, s, state, &op.Rule)
	if err != nil {
		return State{}, err
	}
	return *next, nil
}

type Op_DropRule struct {
	RuleID CID
}

func (op Op_DropRule) isOp() {}

func (op Op_DropRule) Marshal(out []byte) []byte {
	return op.RuleID[:]
}

func (op *Op_DropRule) Unmarshal(data []byte) error {
	if len(data) != 32 {
		return fmt.Errorf("invalid rule cid length: %d", len(data))
	}
	copy(op.RuleID[:], data)
	return nil
}

func (op Op_DropRule) OpCode() OpCode {
	return OpCode_DropRule
}

func (op Op_DropRule) perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error) {
	rule, err := m.GetRule(ctx, s, state, op.RuleID)
	if err != nil {
		return State{}, err
	}
	// TODO: figure out the correct arguments for this.
	yes, err := m.CanAnyDo(ctx, s, state, approvers, Verb_ADMIN, rule.ObjectType, "")
	if err != nil {
		return State{}, err
	}
	if !yes {
		return State{}, fmt.Errorf("cannot drop rule")
	}
	next, err := m.DropRule(ctx, s, state, op.RuleID)
	if err != nil {
		return State{}, err
	}
	return next, nil
}

// Op_PutEntry creates or overwrites a Branch entry.
type Op_PutEntry struct {
	Entry Entry
}

func (op Op_PutEntry) isOp() {}

func (op Op_PutEntry) Marshal(out []byte) []byte {
	out = appendLP(out, op.Entry.Key(nil))
	out = appendLP(out, op.Entry.Value(nil))
	return out
}

func (op *Op_PutEntry) Unmarshal(data []byte) error {
	k, data, err := readLP(data)
	if err != nil {
		return err
	}
	val, _, err := readLP(data)
	if err != nil {
		return err
	}
	op.Entry, err = ParseEntry(k, val)
	if err != nil {
		return err
	}
	return nil
}

func (op Op_PutEntry) OpCode() OpCode {
	return OpCode_PutEntry
}

func (op Op_PutEntry) perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error) {
	// TODO: check permissions.
	next, err := m.PutEntry(ctx, s, state, op.Entry)
	if err != nil {
		return State{}, err
	}
	return *next, nil
}

// Op_DeleteEntry deletes a Branch entry.
type Op_DeleteEntry struct {
	Name string
}

func (op Op_DeleteEntry) isOp() {}

func (op Op_DeleteEntry) Marshal(out []byte) []byte {
	return append(out, op.Name...)
}

func (op *Op_DeleteEntry) Unmarshal(data []byte) error {
	op.Name = string(data)
	return nil
}

func (op Op_DeleteEntry) OpCode() OpCode {
	return OpCode_DeleteEntry
}

func (op Op_DeleteEntry) perform(ctx context.Context, m *Machine, s stores.RW, state State, approvers IDSet) (State, error) {
	// TODO: check permissions.
	next, err := m.DeleteEntry(ctx, s, state, op.Name)
	if err != nil {
		return State{}, err
	}
	return *next, nil
}
