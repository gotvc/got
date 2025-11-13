package gotnsop

import (
	"encoding/binary"
	"fmt"
	"maps"
	"slices"

	"github.com/gotvc/got/src/internal/sbe"
	"go.inet256.org/inet256/src/inet256"
)

type Code uint8

const (
	Code_UNKNOWN Code = iota

	Code_ChangeSet

	Code_CreateGroup
	Code_CreateLeaf
	Code_AddLeaf
	Code_RemoveLeaf
	Code_AddMember
	Code_RemoveMember

	Code_AddRule
	Code_DropRule

	Code_PutEntry
	Code_DeleteEntry
)

type OpHeader [4]byte

func NewOpHeader(code Code, payloadLen int) (ret OpHeader) {
	if payloadLen < 0 || payloadLen > 0x00ffffff {
		panic(fmt.Errorf("payload length out of range: %d", payloadLen))
	}
	h := uint32(code)<<24 | uint32(payloadLen)&0x00ffffff
	binary.LittleEndian.PutUint32(ret[:], h)
	return ret
}

func (h OpHeader) Code() Code {
	return Code(binary.LittleEndian.Uint32(h[:]) >> 24)
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

// Op is a single operation on the ledger.
// Ops are batched into Deltas, which represent an atomic state transition of the ledger.
type Op interface {
	// Marshal marshales the op body.  The header is not included.
	Marshal(out []byte) []byte
	// Unmarshal unmarshals the op body, which does not include the header.
	Unmarshal(data []byte) error
	// Code returns the op code.
	Code() Code

	// validate checks if the op was correctly applied from prev to next.
	//validate(ctx context.Context, m *Machine, s stores.RW, prev, next State, approvers IDSet) error

	isOp()
}

// AppendOp appends the op to the output.
// It calls marshal on the op, and prefixes it with the op header.
func AppendOp(out []byte, op Op) []byte {
	payload := op.Marshal(nil)
	header := NewOpHeader(op.Code(), len(payload))
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

func parseOp(code Code, payload []byte) (Op, error) {
	var op Op
	switch code {
	case Code_ChangeSet:
		op = &ChangeSet{}
	case Code_CreateGroup:
		op = &CreateGroup{}
	case Code_CreateLeaf:
		op = &CreateLeaf{}
	case Code_AddLeaf:
		op = &AddLeaf{}
	case Code_RemoveLeaf:
		op = &RemoveLeaf{}
	case Code_AddMember:
		op = &AddMember{}
	case Code_RemoveMember:
		op = &RemoveMember{}
	case Code_AddRule:
		op = &AddRule{}
	case Code_DropRule:
		op = &DropRule{}

	case Code_PutEntry:
		op = &PutEntry{}
	case Code_DeleteEntry:
		op = &DeleteEntry{}
	default:
		return nil, fmt.Errorf("unrecognized op code: %d", code)
	}
	return op, op.Unmarshal(payload)
}

// ChangeSet is a set of operations signed together.
type ChangeSet struct {
	Ops  []Op
	Sigs map[inet256.ID][]byte
}

func (op ChangeSet) isOp() {}

func (op ChangeSet) Code() Code {
	return Code_ChangeSet
}

// OpData is the portion of the change set that contains the operations.
// This is what is signed.
// The other portion is the sig map.
func (op ChangeSet) OpData(out []byte) []byte {
	out = binary.AppendUvarint(out, uint64(len(op.Ops)))
	for _, op := range op.Ops {
		out = AppendOp(out, op)
	}
	return out
}

func (op ChangeSet) Marshal(out []byte) []byte {
	out = op.OpData(out)
	out = MarshalIDMap(out, op.Sigs)
	return out
}

func (cs *ChangeSet) Unmarshal(data []byte) error {
	// read ops from the beginning.
	opsLen, n := binary.Uvarint(data)
	if n <= 0 {
		return fmt.Errorf("invalid ops length len(data)=%d", len(data))
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
	if err := UnmarshalIDMap(data, sigs); err != nil {
		return err
	}
	cs.Ops = ops
	cs.Sigs = sigs
	return nil
}

// func (cs ChangeSet) validate(ctx context.Context, m *Machine, s stores.RW, prev, next State, approvers IDSet) error {
// 	// collect all of the public keys that we need.
// 	pubKeys := make(map[inet256.ID]inet256.PublicKey)

// 	// Any create operations will provide public keys that are not yet in the state.
// 	for _, op := range cs.Ops {
// 		switch op := op.(type) {
// 		case *CreateLeaf:
// 			pubKeys[op.Leaf.ID] = op.Leaf.PublicKey
// 		}
// 	}
// 	for id := range cs.Sigs {
// 		if _, ok := pubKeys[id]; !ok {
// 			leaf, err := m.GetLeaf(ctx, s, prev, id)
// 			if err != nil {
// 				return err
// 			}
// 			pubKeys[id] = leaf.PublicKey
// 		}
// 	}
// 	// validate the sigs.
// 	target := cs.OpData(nil)
// 	for id, sig := range cs.Sigs {
// 		pubKey := pubKeys[id]
// 		if !pki.Verify(&sigCtxTxn, pubKey, target, sig) {
// 			return fmt.Errorf("invalid signature for %v", id)
// 		}
// 		// add to approvers.
// 		approvers[id] = struct{}{}
// 	}
// 	// validate the ops.
// 	state := prev
// 	for _, op := range cs.Ops {
// 		if err := op.validate(ctx, m, s, state, next, approvers); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

var sigCtxTxn = inet256.SigCtxString("gotns/txn")

// Sign signs the change set with the private key and adds the signature to the sigs map.
func (op *ChangeSet) Sign(pk inet256.PrivateKey) {
	id := pki.NewID(pk.Public().(inet256.PublicKey))
	data := op.OpData(nil)
	sig := pki.Sign(&sigCtxTxn, pk, data, nil)

	if op.Sigs == nil {
		op.Sigs = make(map[inet256.ID][]byte)
	}
	op.Sigs[id] = sig
}

// CreateGroup creates a group.
// It has to be
type CreateGroup struct {
	Group Group
}

func (op CreateGroup) isOp() {}

func (op CreateGroup) Marshal(out []byte) []byte {
	out = sbe.AppendLP(out, op.Group.Key(nil))
	out = sbe.AppendLP(out, op.Group.Value(nil))
	return out
}

func (op *CreateGroup) Unmarshal(data []byte) error {
	k, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	val, _, err := sbe.ReadLP(data)
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

func (op CreateGroup) Code() Code {
	return Code_CreateGroup
}

// func (op CreateGroup) validate(ctx context.Context, m *Machine, s stores.RW, prev, next State, approvers IDSet) error {
// 	_, err := m.GetGroup(ctx, s, prev, op.Group.Name)
// 	if err == nil {
// 		return fmt.Errorf("group already exists")
// 	}
// 	g, err := m.GetGroup(ctx, s, prev, op.Group.Name)
// 	if err != nil {
// 		return err
// 	}
// 	if g.KEM.Equal(op.Group.KEM) {
// 		return fmt.Errorf("group KEM mismatch")
// 	}
// 	return nil
// }

// CreateLeaf creates a new leaf, or fails if the leaf already exists.
type CreateLeaf struct {
	Leaf IdentityLeaf
}

func (op CreateLeaf) isOp() {}

func (op CreateLeaf) Code() Code {
	return Code_CreateLeaf
}

func (op CreateLeaf) Marshal(out []byte) []byte {
	out = sbe.AppendLP(out, op.Leaf.Key(nil))
	out = sbe.AppendLP(out, op.Leaf.Value(nil))
	return out
}

func (op *CreateLeaf) Unmarshal(data []byte) error {
	k, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	val, _, err := sbe.ReadLP(data)
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

// func (op CreateLeaf) validate(ctx context.Context, m *Machine, s stores.RW, prev, next State, approvers IDSet) error {
// 	if _, err := m.GetLeaf(ctx, s, prev, op.Leaf.ID); err == nil {
// 		return fmt.Errorf("leaf already exists")
// 	}
// 	leaf, err := m.GetLeaf(ctx, s, next, op.Leaf.ID)
// 	if err != nil {
// 		return err
// 	}
// 	if !leaf.PublicKey.Equal(op.Leaf.PublicKey) {
// 		return fmt.Errorf("leaf public key mismatch")
// 	}
// 	if _, ok := approvers[op.Leaf.ID]; !ok {
// 		return fmt.Errorf("cannot create leaf without approval from %v", op.Leaf.ID)
// 	}
// 	return nil
// }

// AddLeaf adds a leaf to a group.
// The leaf must exist.
// It can be created by CreateLeaf included in the same transaction.
type AddLeaf struct {
	Group  string
	LeafID inet256.ID
}

func (op AddLeaf) isOp() {}

func (op AddLeaf) Marshal(out []byte) []byte {
	out = append(out, op.Group...)
	out = append(out, op.LeafID[:]...)
	return out
}

func (op *AddLeaf) Unmarshal(data []byte) error {
	if len(data) < 32 {
		return fmt.Errorf("invalid leaf id length: %d", len(data))
	}
	group := string(data[:len(data)-32])
	id := inet256.IDFromBytes(data[len(data)-32:])
	op.Group = group
	op.LeafID = id
	return nil
}

func (op AddLeaf) Code() Code {
	return Code_AddLeaf
}

// func (op AddLeaf) validate(ctx context.Context, m *Machine, s stores.RW, prev, next State, approvers IDSet) error {
// 	_, err := m.GetLeaf(ctx, s, prev, op.LeafID)
// 	if err != nil {
// 		return err
// 	}
// 	panic("not implemented")
// }

// RemoveLeaf removes a leaf from a group.
// If the leaf is not in any group, it is removed from the leaves table.
type RemoveLeaf struct {
	Group string
	ID    inet256.ID
}

func (op RemoveLeaf) isOp() {}

func (op RemoveLeaf) Marshal(out []byte) []byte {
	out = append(out, op.Group...)
	out = append(out, op.ID[:]...)
	return out
}

func (op *RemoveLeaf) Unmarshal(data []byte) error {
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

func (op RemoveLeaf) Code() Code {
	return Code_RemoveLeaf
}

type AddMember struct {
	Group, Member string
	EncryptedKEM  []byte
}

func (op AddMember) isOp() {}

func (op AddMember) Marshal(out []byte) []byte {
	out = sbe.AppendLP(out, []byte(op.Group))
	out = sbe.AppendLP(out, []byte(op.Member))
	out = sbe.AppendLP(out, op.EncryptedKEM)
	return out
}

func (op *AddMember) Unmarshal(data []byte) error {
	group, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	member, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	encryptedKEM, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	op.Group = string(group)
	op.Member = string(member)
	op.EncryptedKEM = encryptedKEM
	return nil
}

func (op AddMember) Code() Code {
	return Code_AddMember
}

// func (op AddMember) validate(ctx context.Context, m *Machine, s stores.RW, prev, next State, approvers IDSet) error {
// 	g, err := m.GetGroup(ctx, s, prev, op.Group)
// 	if err != nil {
// 		return err
// 	}
// 	foundOwner := false
// 	for _, owner := range g.Owners {
// 		if _, exists := approvers[owner]; exists {
// 			foundOwner = true
// 			break
// 		}
// 	}
// 	if !foundOwner {
// 		return fmt.Errorf("cannot add member without approval from an owner")
// 	}

// 	g, err = m.GetGroup(ctx, s, next, op.Group)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

type RemoveMember struct {
	Group, Member string
}

func (op RemoveMember) isOp() {}

func (op RemoveMember) Marshal(out []byte) []byte {
	out = sbe.AppendLP(out, []byte(op.Group))
	out = sbe.AppendLP(out, []byte(op.Member))
	return out
}

func (op *RemoveMember) Unmarshal(data []byte) error {
	group, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	member, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	op.Group = string(group)
	op.Member = string(member)
	return nil
}

func (op RemoveMember) Code() Code {
	return Code_RemoveMember
}

type AddRule struct {
	Rule Rule
}

func (op AddRule) isOp() {}

func (op AddRule) Marshal(out []byte) []byte {
	out = op.Rule.Marshal(out)
	return out
}

func (op *AddRule) Unmarshal(data []byte) error {
	return op.Rule.Unmarshal(data)
}

func (op AddRule) Code() Code {
	return Code_AddRule
}

// func (op AddRule) validate(ctx context.Context, m *Machine, s stores.RW, prev, next State, approvers IDSet) error {
// 	yes, err := m.CanAnyDo(ctx, s, prev, approvers, "ADMIN", op.Rule.ObjectType, op.Rule.Names.String())
// 	if err != nil {
// 		return err
// 	}
// 	if !yes {
// 		return fmt.Errorf("cannot add rule")
// 	}
// 	return nil
// }

type DropRule struct {
	RuleID RuleID
}

func (op DropRule) isOp() {}

func (op DropRule) Marshal(out []byte) []byte {
	return op.RuleID[:]
}

func (op *DropRule) Unmarshal(data []byte) error {
	if len(data) != 32 {
		return fmt.Errorf("invalid rule cid length: %d", len(data))
	}
	copy(op.RuleID[:], data)
	return nil
}

func (op DropRule) Code() Code {
	return Code_DropRule
}

// PutEntry creates or overwrites a Branch entry.
type PutEntry struct {
	Entry Entry
}

func (op PutEntry) isOp() {}

func (op PutEntry) Marshal(out []byte) []byte {
	out = sbe.AppendLP(out, op.Entry.Key(nil))
	out = sbe.AppendLP(out, op.Entry.Value(nil))
	return out
}

func (op *PutEntry) Unmarshal(data []byte) error {
	k, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	val, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	op.Entry, err = ParseEntry(k, val)
	if err != nil {
		return err
	}
	return nil
}

func (op PutEntry) Code() Code {
	return Code_PutEntry
}

// DeleteEntry deletes a Branch entry.
type DeleteEntry struct {
	Name string
}

func (op DeleteEntry) isOp() {}

func (op DeleteEntry) Marshal(out []byte) []byte {
	return append(out, op.Name...)
}

func (op *DeleteEntry) Unmarshal(data []byte) error {
	op.Name = string(data)
	return nil
}

func (op DeleteEntry) Code() Code {
	return Code_DeleteEntry
}

func MarshalIDMap(out []byte, leaves map[inet256.ID][]byte) []byte {
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

func UnmarshalIDMap(data []byte, dst map[inet256.ID][]byte) error {
	clear(dst)
	var lastID inet256.ID
	for len(data) > 0 {
		ent, rest, err := sbe.ReadLP(data)
		if err != nil {
			return err
		}
		if len(ent) < inet256.AddrSize {
			return fmt.Errorf("map entry cannot be less than 32 bytes. %d", len(ent))
		}
		id := inet256.IDFromBytes(ent[:32])
		if compareLeafIDs(id, lastID) <= 0 {
			return fmt.Errorf("leaves are not sorted")
		}
		// insert into the map
		dst[id] = ent[inet256.AddrSize:]

		lastID = id
		data = rest
	}
	return nil
}
