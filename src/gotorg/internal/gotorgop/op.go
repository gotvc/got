package gotorgop

import (
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/sbe"
	"go.inet256.org/inet256/src/inet256"
)

type Code uint8

const (
	Code_UNKNOWN Code = iota

	Code_ChangeSet

	Code_CreateGroup
	Code_CreateIDUnit
	Code_AddMember
	Code_RemoveMember

	Code_AddRule
	Code_DropRule

	Code_AddVolume
	Code_DropVolume
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

	Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error

	isOp()
}

type State interface {
	CanDo(ctx context.Context, actor inet256.ID, verb Verb, objType ObjectType, objName string) (bool, error)
	GetLeaf(ctx context.Context, id inet256.ID) (*IdentityUnit, error)
}

// Diff is things that have changed between two system states.
type Diff interface {
	ForEachRule(ctx context.Context, fn func(rule Rule) error) error
	ForEachVolumeEntry(ctx context.Context, fn func(entry VolumeEntry) error) error
	ForEachBranchEntry(ctx context.Context, fn func(entry VolumeAlias) error) error
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
	case Code_CreateIDUnit:
		op = &CreateIDUnit{}
	case Code_AddMember:
		op = &AddMember{}
	case Code_RemoveMember:
		op = &RemoveMember{}
	case Code_AddRule:
		op = &AddRule{}
	case Code_DropRule:
		op = &DropRule{}

	case Code_AddVolume:
		op = &AddVolume{}
	case Code_DropVolume:
		op = &DropVolume{}
	case Code_PutEntry:
		op = &PutBranchEntry{}
	case Code_DeleteEntry:
		op = &DeleteBranchEntry{}
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

func (cs ChangeSet) Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error {
	// collect all of the public keys that we need.
	pubKeys := make(map[inet256.ID]inet256.PublicKey)

	// Any create operations will provide public keys that are not yet in the state.
	for _, op := range cs.Ops {
		switch op := op.(type) {
		case *CreateIDUnit:
			pubKeys[op.Unit.ID] = op.Unit.SigPublicKey
		}
	}
	for id := range cs.Sigs {
		if _, ok := pubKeys[id]; !ok {
			leaf, err := prev.GetLeaf(ctx, id)
			if err != nil {
				return err
			}
			pubKeys[id] = leaf.SigPublicKey
		}
	}
	// validate the sigs.
	target := cs.OpData(nil)
	for id, sig := range cs.Sigs {
		pubKey := pubKeys[id]
		if !pki.Verify(&sigCtxTxn, pubKey, target, sig) {
			return fmt.Errorf("invalid signature for %v", id)
		}
		// add to approvers.
		approvers.Add(id)
	}
	// validate the ops.
	for _, op := range cs.Ops {
		if err := op.Validate(ctx, prev, diff, approvers); err != nil {
			return err
		}
	}
	return nil
}

var sigCtxTxn = inet256.SigCtxString("gotorg/txn")

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

func (op CreateGroup) Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error {
	return nil
}

// CreateIDUnit creates a new leaf, or fails if the leaf already exists.
type CreateIDUnit struct {
	Unit IdentityUnit
}

func (op CreateIDUnit) isOp() {}

func (op CreateIDUnit) Code() Code {
	return Code_CreateIDUnit
}

func (op CreateIDUnit) Marshal(out []byte) []byte {
	out = sbe.AppendLP(out, op.Unit.Key(nil))
	out = sbe.AppendLP(out, op.Unit.Value(nil))
	return out
}

func (op *CreateIDUnit) Unmarshal(data []byte) error {
	k, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	val, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	unit, err := ParseIDUnit(k, val)
	if err != nil {
		return err
	}
	op.Unit = *unit
	return nil
}

func (op CreateIDUnit) Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error {

	return nil
}

type AddMember struct {
	Group        GroupID
	Member       Member
	EncryptedKEM []byte
}

func (op AddMember) isOp() {}

func (op AddMember) Marshal(out []byte) []byte {
	out = sbe.AppendLP(out, op.Group[:])
	out = sbe.AppendLP(out, op.Member.Marshal(nil))
	out = sbe.AppendLP(out, op.EncryptedKEM)
	return out
}

func (op *AddMember) Unmarshal(data []byte) error {
	group, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	memberData, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	encryptedKEM, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	var member Member
	if err := member.Unmarshal(memberData); err != nil {
		return err
	}

	op.Member = member
	op.Group = GroupID(group)
	op.EncryptedKEM = encryptedKEM
	return nil
}

func (op AddMember) Code() Code {
	return Code_AddMember
}

func (op AddMember) Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error {

	return nil
}

type RemoveMember struct {
	Group  GroupID
	Member Member
}

func (op RemoveMember) isOp() {}

func (op RemoveMember) Marshal(out []byte) []byte {
	out = sbe.AppendLP(out, op.Group[:])
	out = sbe.AppendLP(out, op.Member.Marshal(nil))
	return out
}

func (op *RemoveMember) Unmarshal(data []byte) error {
	group, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	memberData, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	var member Member
	if err := member.Unmarshal(memberData); err != nil {
		return err
	}

	op.Group = GroupID(group)
	op.Member = member
	return nil
}

func (op RemoveMember) Code() Code {
	return Code_RemoveMember
}

func (op RemoveMember) Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error {
	return nil
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

func (op AddRule) Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error {
	yes := false
	for _, approver := range approvers {
		y, err := prev.CanDo(ctx, approver, Verb_ADMIN, op.Rule.ObjectType, "")
		if err != nil {
			return err
		}
		yes = yes || y
		if yes {
			break
		}
	}
	if !yes {
		return fmt.Errorf("cannot add rule")
	}
	return nil
}

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

func (op DropRule) Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error {
	return nil
}

var _ Op = &AddVolume{}

type AddVolume struct {
	Volume blobcache.OID
}

func (op AddVolume) isOp() {}

func (op AddVolume) Marshal(out []byte) []byte {
	// AddVolume has no fields to marshal.
	return out
}

func (op *AddVolume) Unmarshal(data []byte) error {
	// AddVolume has no fields to unmarshal.
	return nil
}

func (op AddVolume) Code() Code {
	return Code_AddVolume
}

func (op AddVolume) Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error {
	// should check that volume did not exist previously
	// and that all obligations are met for the new volume.
	return nil
}

var _ Op = &DropVolume{}

type DropVolume struct {
	Volume blobcache.OID
}

func (op DropVolume) isOp() {}

func (op DropVolume) Marshal(out []byte) []byte {
	// DropVolume has no fields to marshal.
	return out
}

func (op *DropVolume) Unmarshal(data []byte) error {
	// DropVolume has no fields to unmarshal.
	return nil
}

func (op DropVolume) Code() Code {
	return Code_DropVolume
}

func (op DropVolume) Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error {
	return nil
}

// PutBranchEntry creates or overwrites a Branch entry.
type PutBranchEntry struct {
	Name   string
	Volume blobcache.OID
}

func (op PutBranchEntry) isOp() {}

func (op PutBranchEntry) Marshal(out []byte) []byte {
	out = sbe.AppendLP(out, []byte(op.Name))
	out = append(out, op.Volume[:]...)
	return out
}

func (op *PutBranchEntry) Unmarshal(data []byte) error {
	k, data, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	val, _, err := sbe.ReadN(data, blobcache.OIDSize)
	if err != nil {
		return err
	}
	op.Name = string(k)
	copy(op.Volume[:], val)
	return nil
}

func (op PutBranchEntry) Code() Code {
	return Code_PutEntry
}

func (op PutBranchEntry) Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error {
	return nil
}

// DeleteEntry deletes a Branch entry.
type DeleteBranchEntry struct {
	Name string
}

func (op DeleteBranchEntry) isOp() {}

func (op DeleteBranchEntry) Marshal(out []byte) []byte {
	return append(out, op.Name...)
}

func (op *DeleteBranchEntry) Unmarshal(data []byte) error {
	op.Name = string(data)
	return nil
}

func (op DeleteBranchEntry) Code() Code {
	return Code_DeleteEntry
}

func (op DeleteBranchEntry) Validate(ctx context.Context, prev State, diff Diff, approvers IDSet) error {
	return nil
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
