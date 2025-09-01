// package gotled implements an append-only ledger where state transitions can be verified
package gotled

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/gotvc/got/src/internal/merklelog"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

type Marshaler interface {
	// Marshal appends the marshaled representation of the object to out
	// and returns the new slice (of which out is a prefix).
	Marshal(out []byte) []byte
}

type Root[State, Delta Marshaler] struct {
	// History is the history of all previous states.
	// The most recent delta is referenced by the last element of the history.
	History merklelog.State
	// Delta is the set of changes applied to get the current state from the previous state.
	Delta Delta
	// State is the current state of the system.
	State State
}

// 3 x 32 bit integers for the length of the state, delta, and history
const headerSize = 4 * 3

func readHeader(data []byte) (h [3]uint32, rest []byte, _ error) {
	if len(data) < headerSize {
		return [3]uint32{}, nil, fmt.Errorf("gotled: invalid data, must be at least 12 bytes")
	}
	header := [3]uint32{}
	for i := 0; i < 3; i++ {
		header[i] = binary.BigEndian.Uint32(data[i*4 : (i+1)*4])
	}
	return header, data[headerSize:], nil
}

func (r Root[State, Delta]) Marshal(out []byte) []byte {
	offset := len(out)
	for i := 0; i < headerSize; i++ {
		out = append(out, 0)
	}
	header := [3]uint32{}
	for i, m := range []Marshaler{r.History, r.Delta, r.State} {
		l1 := len(out)
		out = m.Marshal(out)
		l2 := len(out)
		header[i] = uint32(l2 - l1)
	}
	for i := range header {
		binary.BigEndian.PutUint32(out[offset+i*4:], header[i])
	}
	return out
}

type Parser[T any] = func(data []byte) (T, error)

func Parse[State, Delta Marshaler](data []byte, parseState Parser[State], parseDelta Parser[Delta]) (Root[State, Delta], error) {
	header, data, err := readHeader(data)
	if err != nil {
		return Root[State, Delta]{}, err
	}
	historyLen := header[0]
	deltaLen := header[1]
	stateLen := header[2]
	totalLen := stateLen + deltaLen + historyLen
	if len(data) < int(totalLen) {
		return Root[State, Delta]{}, fmt.Errorf("gotled: invalid data, must be at least %d bytes", totalLen)
	}
	history, err := merklelog.Parse(data[:historyLen])
	if err != nil {
		return Root[State, Delta]{}, err
	}
	delta, err := parseDelta(data[historyLen : historyLen+deltaLen])
	if err != nil {
		return Root[State, Delta]{}, err
	}
	state, err := parseState(data[historyLen+deltaLen : historyLen+deltaLen+stateLen])
	if err != nil {
		return Root[State, Delta]{}, err
	}
	return Root[State, Delta]{
		State:   state,
		Delta:   delta,
		History: history,
	}, nil
}

// Machine performs operations on a ledger.
type Machine[State, Delta Marshaler] struct {
	// Apply must produce the result of applying the delta to the state.
	Apply func(ctx context.Context, s stores.RW, prev State, change Delta) (State, error)
	// ParseState parses a State from a byte slice.
	ParseState Parser[State]
	// ParseDelta parses a Delta from a byte slice.
	ParseDelta Parser[Delta]
}

// Parse parses a Root from a byte slice.
func (m *Machine[State, Delta]) Parse(data []byte) (Root[State, Delta], error) {
	return Parse(data, m.ParseState, m.ParseDelta)
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
	if root.History.Len() == slot {
		return &root, nil
	}
	if slot >= root.History.Len() {
		return nil, fmt.Errorf("gotled: slot %d out of bounds (length %d)", slot, root.History.Len())
	}
	prevStateCID, err := merklelog.Get(ctx, s, root.History, slot)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, s.MaxSize())
	n, err := s.Get(ctx, prevStateCID, buf)
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

func (m *Machine[State, Delta]) Validate(ctx context.Context, s stores.Reading, prev Root[State, Delta], next Root[State, Delta]) error {
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
		nextState, err := m.Apply(ctx, s2, prevRoot.State, root.Delta)
		if err != nil {
			return err
		}
		if !bytes.Equal(nextState.Marshal(nil), root.State.Marshal(nil)) {
			return fmt.Errorf("gotled: next state does not match expected state")
		}
	}
	return nil
}

// AndThen creates a new root with the given delta, and state.
// History is updated to reflect the previous state.
// AndThen does not modify the current root, it returns a new root.
func (m *Machine[State, Delta]) AndThen(ctx context.Context, s stores.RW, r Root[State, Delta], delta Delta, next State) (Root[State, Delta], error) {
	prevCID, err := s.Post(ctx, r.State.Marshal(nil))
	if err != nil {
		return Root[State, Delta]{}, err
	}
	r2 := r
	r2.State = next
	r2.Delta = delta
	if err := r2.History.Append(ctx, s, prevCID); err != nil {
		return Root[State, Delta]{}, err
	}
	return r2, nil
}

func (m *Machine[State, Delta]) NewIterator(s stores.Reading, prev Root[State, Delta], next Root[State, Delta]) *Iterator[State, Delta] {
	return &Iterator[State, Delta]{
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
	root, err := Parse(data, it.m.ParseState, it.m.ParseDelta)
	if err != nil {
		return err
	}
	*dst = root
	return nil
}
