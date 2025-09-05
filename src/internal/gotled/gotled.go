// package gotled implements an append-only ledger where state transitions can be verified
package gotled

import (
	"context"
	"encoding/binary"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/merklelog"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

type Marshaler interface {
	// Marshal appends the marshaled representation of the object to out
	// and returns the new slice (of which out is a prefix).
	Marshal(out []byte) []byte
}

// Root is contains transitive references to the entire history and current state of the system.
type Root[State, Proof Marshaler] struct {
	// History is the history of all previous states.
	// The most recent delta is referenced by the last element of the history.
	History merklelog.State
	// State is the current state of the system.
	State State
	// Proof is a proof that this state is a valid next state given the previous history.
	// The simplest way to do this is a delta from the previous state.
	Proof Proof
}

// 8 bits for the history length, 24 bits for the length of the state.
const headerSize = 4

func readHeader(data []byte) (h uint32, rest []byte, _ error) {
	if len(data) < headerSize {
		return 0, nil, fmt.Errorf("gotled: invalid data, must be at least 12 bytes")
	}
	header := binary.LittleEndian.Uint32(data)
	return header, data[headerSize:], nil
}

func (r Root[State, Delta]) Marshal(out []byte) []byte {
	offset := len(out)
	for i := 0; i < headerSize; i++ {
		out = append(out, 0)
	}
	out = r.History.Marshal(out)
	preStateLen := len(out)
	out = r.State.Marshal(out)
	postStateLen := len(out)
	header := uint32(len(r.History.Levels)) | uint32(postStateLen-preStateLen)<<8
	// Use little endian as a convention because the merklelog levels are also stored in little endian order.
	binary.LittleEndian.PutUint32(out[offset:offset+headerSize], header)

	out = r.Proof.Marshal(out)
	return out
}

func (r Root[State, Proof]) Len() merklelog.Pos {
	return r.History.Len()
}

type Parser[T any] = func(data []byte) (T, error)

func Parse[State, Proof Marshaler](data []byte, parseState Parser[State], parseProof Parser[Proof]) (Root[State, Proof], error) {
	header, data, err := readHeader(data)
	if err != nil {
		return Root[State, Proof]{}, err
	}
	historyLen := (header & 0xff) * blobcache.CIDSize
	stateLen := header >> 8
	history, err := merklelog.Parse(data[:historyLen])
	if err != nil {
		return Root[State, Proof]{}, err
	}
	state, err := parseState(data[historyLen : historyLen+stateLen])
	if err != nil {
		return Root[State, Proof]{}, err
	}
	proof, err := parseProof(data[historyLen+stateLen:])
	if err != nil {
		return Root[State, Proof]{}, err
	}
	return Root[State, Proof]{
		State:   state,
		Proof:   proof,
		History: history,
	}, nil
}

// Machine performs operations on a ledger.
type Machine[State, Proof Marshaler] struct {
	// ParseState parses a State from a byte slice.
	ParseState Parser[State]
	// ParseProof parses a Proof from a byte slice.
	ParseProof Parser[Proof]
	// Verify verifies that prev -> next is a valid transition using the given proof.
	// Verify must return nil only if the transition is valid, and never needs to be considered again.
	Verify func(ctx context.Context, s stores.Reading, prev, next State, proof Proof) error
}

// Parse parses a Root from a byte slice.
func (m *Machine[State, Delta]) Parse(data []byte) (Root[State, Delta], error) {
	return Parse(data, m.ParseState, m.ParseProof)
}

// Initial creates a new root with the given state, and an empty history.
func (m *Machine[State, Delta]) Initial(initState State) Root[State, Delta] {
	return Root[State, Delta]{
		State:   initState,
		History: merklelog.State{},
	}
}

func (m *Machine[State, Delta]) PostRoot(ctx context.Context, s stores.Writing, root Root[State, Delta]) (merklelog.CID, error) {
	return s.Post(ctx, root.Marshal(nil))
}

func (m *Machine[State, Delta]) GetRoot(ctx context.Context, s stores.Reading, cid merklelog.CID) (*Root[State, Delta], error) {
	buf := make([]byte, s.MaxSize())
	n, err := s.Get(ctx, cid, buf)
	if err != nil {
		return nil, err
	}
	ret, err := m.Parse(buf[:n])
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

// Slot returns the state at the given position in the history.
func (m *Machine[State, Delta]) Slot(ctx context.Context, s stores.Reading, root Root[State, Delta], slot merklelog.Pos) (*Root[State, Delta], error) {
	switch {
	case root.History.Len() == slot:
		return &root, nil
	case slot > root.History.Len():
		return nil, fmt.Errorf("gotled: slot %d out of bounds (length %d)", slot, root.History.Len())
	}
	cid, err := merklelog.Get(ctx, s, root.History, slot)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, s.MaxSize())
	n, err := s.Get(ctx, cid, buf)
	if err != nil {
		return nil, err
	}
	ret, err := m.Parse(buf[:n])
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

// GetPrev returns the root, immediately previous to the given root.
func (m *Machine[State, Delta]) GetPrev(ctx context.Context, s stores.Reading, root Root[State, Delta]) (*Root[State, Delta], error) {
	return m.Slot(ctx, s, root, root.History.Len()-1)
}

func (m *Machine[State, Delta]) Validate(ctx context.Context, s stores.Reading, prev, next Root[State, Delta]) error {
	// First, the next root must include the previous root.
	// If the next root does not acknowledge all of the history that we already know is true
	// then it can't be a correct continuation.
	yesInc, err := merklelog.Includes(ctx, s, next.History, prev.History)
	if err != nil {
		return err
	}
	if !yesInc {
		return fmt.Errorf("gotled: next root does not include history of previous root")
	}
	return nil // TODO
	// Next, we iterate through all of the intermediate states, and check that the transition is valid.
	it := m.NewIterator(s, prev, next)
	for {
		var root Root[State, Delta]
		if err := it.Next(ctx, &root); err != nil {
			if streams.IsEOS(err) {
				break
			}
			return err
		}
		prevRoot, err := m.GetPrev(ctx, s, root)
		if err != nil {
			return err
		}
		s2 := stores.AddWriteLayer(s, stores.NewMem())
		prevState := prevRoot.State
		nextState := root.State
		if err := m.Verify(ctx, s2, prevState, nextState, root.Proof); err != nil {
			return err
		}
	}
	return nil
}

// AndThen creates a new root with the given delta, and state.
// History is updated to reflect the previous state.
// AndThen does not modify the current root, it returns a new root.
func (m *Machine[State, Proof]) AndThen(ctx context.Context, s stores.RW, r Root[State, Proof], proof Proof, next State) (Root[State, Proof], error) {
	prevCID, err := s.Post(ctx, r.State.Marshal(nil))
	if err != nil {
		return Root[State, Proof]{}, err
	}
	r2 := r
	r2.Proof = proof
	r2.State = next
	if err := r2.History.Append(ctx, s, prevCID); err != nil {
		return Root[State, Proof]{}, err
	}
	return r2, nil
}

func (m *Machine[State, Proof]) NewIterator(s stores.Reading, prev Root[State, Proof], next Root[State, Proof]) *Iterator[State, Proof] {
	return &Iterator[State, Proof]{
		m:  m,
		it: merklelog.NewIterator(next.History, s, prev.History.Len(), next.History.Len()),
		s:  s,
	}
}

// Iterator is an iterator over all the intermediate states between two roots.
type Iterator[State, Delta Marshaler] struct {
	m   *Machine[State, Delta]
	it  *merklelog.Iterator
	s   stores.Reading
	buf []byte
}

func (it *Iterator[State, Delta]) Next(ctx context.Context, dst *Root[State, Delta]) error {
	var cid merklelog.CID
	if err := it.it.Next(ctx, &cid); err != nil {
		return err
	}
	if it.buf == nil {
		it.buf = make([]byte, it.s.MaxSize())
	}
	n, err := it.s.Get(ctx, cid, it.buf)
	if err != nil {
		return err
	}
	data := it.buf[:n]
	root, err := Parse(data, it.m.ParseState, it.m.ParseProof)
	if err != nil {
		return err
	}
	*dst = root
	return nil
}
