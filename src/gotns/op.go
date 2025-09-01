package gotns

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/gotvc/got/src/internal/gotled"
	"github.com/gotvc/got/src/internal/stores"
	"go.inet256.org/inet256/src/inet256"
)

// Builder allows a transction to be built up incrementally
// And then turned into a single slot change to the ledger.
type Builder struct {
	m       gotled.Machine[State, Delta]
	prev    Root
	changes []Op
}

// NewBuilder creates a new delta builder.
func (m *Machine) NewBuilder(prev Root) *Builder {
	return &Builder{
		m:       m.led,
		prev:    prev,
		changes: []Op{},
	}
}

func (b *Builder) AddOp(op Op) {
	b.changes = append(b.changes, op)
}

// Finish applies the changes to the previous root, and returns the new root.
func (tx *Builder) Finish(ctx context.Context, s stores.RW) (Root, error) {
	next, err := tx.m.Apply(ctx, s, tx.prev.State, Delta(tx.changes))
	if err != nil {
		return Root{}, err
	}
	return tx.m.AndThen(ctx, s, tx.prev, Delta(tx.changes), next)
}

type OpCode uint8

const (
	OpCode_CreateGroup  OpCode = 1
	OpCode_AddLeaf      OpCode = 2
	OpCode_DropLeaf     OpCode = 3
	OpCode_AddMember    OpCode = 4
	OpCode_RemoveMember OpCode = 5

	OpCode_AddRule  OpCode = 6
	OpCode_DropRule OpCode = 7

	OpCode_PutEntry    OpCode = 8
	OpCode_DeleteEntry OpCode = 9
)

type OpHeader [4]byte

func NewOpHeader(code OpCode, payloadLen int) (ret OpHeader) {
	if payloadLen < 0 || payloadLen > 0x00ffffff {
		panic(fmt.Errorf("payload length out of range: %d", payloadLen))
	}
	h := uint32(code)<<24 | uint32(payloadLen)&0x00ffffff
	binary.BigEndian.PutUint32(ret[:], h)
	return ret
}

func (h OpHeader) Code() OpCode {
	return OpCode(binary.BigEndian.Uint32(h[:]) >> 24)
}

func (h OpHeader) PayloadLen() int {
	return int(binary.BigEndian.Uint32(h[:]) & 0x00ffffff)
}

func readHeader(data []byte) (OpHeader, []byte, error) {
	if len(data) < 4 {
		return OpHeader{}, nil, fmt.Errorf("too short to contain op header")
	}
	var header OpHeader
	copy(header[:], data)
	return header, data[4:], nil
}

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
	perform(ctx context.Context, m *Machine, s stores.RW, state State) (State, error)

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
	case OpCode_CreateGroup:
		op = &Op_CreateGroup{}
	case OpCode_AddLeaf:
		op = &Op_AddLeaf{}
	case OpCode_DropLeaf:
		op = &Op_DropLeaf{}
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

// Op_CreateGroup creates a new group.
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
	k, rest, err := readLP(data)
	if err != nil {
		return err
	}
	data = rest
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

func (op Op_CreateGroup) perform(ctx context.Context, m *Machine, s stores.RW, state State) (State, error) {
	panic("not implemented")
}

type Op_AddLeaf struct {
	Leaf IdentityLeaf
}

func (op Op_AddLeaf) isOp() {}

func (op Op_AddLeaf) Marshal(out []byte) []byte {
	out = appendLP(out, op.Leaf.Key(nil))
	out = appendLP(out, op.Leaf.Value(nil))
	return out
}

func (op *Op_AddLeaf) Unmarshal(data []byte) error {
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

func (op Op_AddLeaf) OpCode() OpCode {
	return OpCode_AddLeaf
}

func (op Op_AddLeaf) perform(ctx context.Context, m *Machine, s stores.RW, state State) (State, error) {
	next, err := m.AddLeaf(ctx, s, state, op.Leaf)
	if err != nil {
		return State{}, err
	}
	return *next, nil
}

type Op_DropLeaf struct {
	Group string
	ID    inet256.ID
}

func (op Op_DropLeaf) isOp() {}

func (op Op_DropLeaf) Marshal(out []byte) []byte {
	return append(out, leafKey(op.Group, op.ID)...)
}

func (op *Op_DropLeaf) Unmarshal(data []byte) error {
	group, id, err := parseLeafKey(data)
	if err != nil {
		return err
	}
	op.Group = group
	op.ID = id
	return nil
}

func (op Op_DropLeaf) OpCode() OpCode {
	return OpCode_DropLeaf
}

func (op Op_DropLeaf) perform(ctx context.Context, m *Machine, s stores.RW, state State) (State, error) {
	next, err := m.DropLeaf(ctx, s, state, op.Group, op.ID)
	if err != nil {
		return State{}, err
	}
	return *next, nil
}

type Op_AddMember struct {
	Group, Member string
}

func (op Op_AddMember) isOp() {}

func (op Op_AddMember) Marshal(out []byte) []byte {
	out = appendLP(out, []byte(op.Group))
	out = appendLP(out, []byte(op.Member))
	return out
}

func (op *Op_AddMember) Unmarshal(data []byte) error {
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

func (op Op_AddMember) OpCode() OpCode {
	return OpCode_AddMember
}

func (op Op_AddMember) perform(ctx context.Context, m *Machine, s stores.RW, state State) (State, error) {
	panic("not implemented")
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

func (op Op_RemoveMember) perform(ctx context.Context, m *Machine, s stores.RW, state State) (State, error) {
	panic("not implemented")
}

type Op_AddRule struct {
	Rule Rule
}

func (op Op_AddRule) isOp() {}

func (op Op_AddRule) Marshal(out []byte) []byte {
	out = appendLP(out, []byte(op.Rule.Subject))
	out = appendLP(out, []byte(op.Rule.Verb))
	out = appendLP(out, op.Rule.Object.Marshal(nil))
	return out
}

func (op *Op_AddRule) Unmarshal(data []byte) error {
	subject, data, err := readLP(data)
	if err != nil {
		return err
	}
	verb, data, err := readLP(data)
	if err != nil {
		return err
	}
	objectData, _, err := readLP(data)
	if err != nil {
		return err
	}
	var objSet ObjectSet
	if err := objSet.Unmarshal(objectData); err != nil {
		return err
	}
	op.Rule.Subject = string(subject)
	op.Rule.Verb = Verb(verb)
	op.Rule.Object = objSet
	return nil
}

func (op Op_AddRule) OpCode() OpCode {
	return OpCode_AddRule
}

func (op Op_AddRule) perform(ctx context.Context, m *Machine, s stores.RW, state State) (State, error) {
	panic("not implemented")
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

func (op Op_DropRule) perform(ctx context.Context, m *Machine, s stores.RW, state State) (State, error) {
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

func (op Op_PutEntry) perform(ctx context.Context, m *Machine, s stores.RW, state State) (State, error) {
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

func (op Op_DeleteEntry) perform(ctx context.Context, m *Machine, s stores.RW, state State) (State, error) {
	next, err := m.DeleteEntry(ctx, s, state, op.Name)
	if err != nil {
		return State{}, err
	}
	return *next, nil
}
