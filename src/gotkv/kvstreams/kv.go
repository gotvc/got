package kvstreams

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"go.brendoncarroll.net/exp/sbe"
	"go.brendoncarroll.net/exp/streams"
)

// Entry is a single entry in a stream/tree
type Entry struct {
	Key, Value []byte
}

// Clone makes a copy of Entry where Key and Value do not point to memory overlapping with the original
func (e Entry) Clone() Entry {
	return Entry{
		Key:   append([]byte{}, e.Key...),
		Value: append([]byte{}, e.Value...),
	}
}

func (e Entry) String() string {
	return fmt.Sprintf("{%q => %q}", e.Key, e.Value)
}

// Iterator iterates over entries
//
// e.g.
//
//	if err := it.Seek(ctx, key); err != nil {
//	  return err
//	}
//
// var ent Entry
//
//	for err := it.Next(ctx, &ent); err != EOS; err = it.Next(ctx, &ent) {
//	  if err != nil {
//		   return err
//	  }
//	  // use ent here. ent will be valid until the next call to it.Next
//	}
type Iterator = streams.Iterator[Entry]

func Peek(ctx context.Context, it streams.Peekable[Entry]) (*Entry, error) {
	var ent Entry
	if err := it.Peek(ctx, &ent); err != nil {
		return nil, err
	}
	return &ent, nil
}

// CopyEntry copies an entry from src to dst.
func CopyEntry(dst *Entry, src Entry) {
	dst.Key = append(dst.Key[:0], src.Key...)
	dst.Value = append(dst.Value[:0], src.Value...)
}

// A span of keys [Begin, End)
// If you want to include a specific end key, use the KeyAfter function.
// nil is interpretted as no bound, not as a 0 length key.  This behaviour is only releveant for End.
type Span struct {
	Begin []byte
	End   []byte
}

// Contains returns true if x is in the Span
func (s Span) Contains(x []byte) bool {
	return !(s.AllGt(x) || s.AllLt(x))
}

// AllLt returns true if every key in the span is less than x
func (s Span) AllLt(x []byte) bool {
	return s.End != nil && bytes.Compare(s.End, x) <= 0
}

// AllGt returns true if every key in the span is greater than x
func (s Span) AllGt(x []byte) bool {
	return bytes.Compare(s.Begin, x) > 0
}

func (s Span) Marshal(out []byte) []byte {
	if s.Begin != nil {
		out = sbe.AppendUint16(out, uint16(len(s.Begin)))
	} else {
		out = sbe.AppendUint16(out, math.MaxUint16)
	}
	if s.End != nil {
		out = sbe.AppendUint16(out, uint16(len(s.End)))
	} else {
		out = sbe.AppendUint16(out, math.MaxUint16)
	}
	out = append(out, s.Begin...)
	out = append(out, s.End...)
	return out
}

func (s *Span) Unmarshal(data []byte) error {
	beginLen, data, err := sbe.ReadUint16(data)
	if err != nil {
		return err
	}
	endLen, data, err := sbe.ReadUint16(data)
	if err != nil {
		return err
	}

	if beginLen != math.MaxUint16 {
		beginData, rest, err := sbe.ReadN(data, int(beginLen))
		if err != nil {
			return err
		}
		s.Begin = append(s.Begin[:0], beginData...)
		data = rest
	} else {
		s.Begin = nil
	}

	if endLen != math.MaxUint16 {
		endData, rest, err := sbe.ReadN(data, int(endLen))
		if err != nil {
			return err
		}
		s.End = append(s.End[:0], endData...)
		data = rest
	} else {
		s.End = nil
	}
	return nil
}

func CloneSpan(x Span) Span {
	var begin, end []byte
	if x.Begin != nil {
		begin = append([]byte{}, x.Begin...)
	}
	if x.End != nil {
		end = append([]byte{}, x.End...)
	}
	return Span{
		Begin: begin,
		End:   end,
	}
}

func TotalSpan() Span {
	return Span{}
}

func SingleItemSpan(x []byte) Span {
	return Span{
		Begin: x,
		End:   KeyAfter(x),
	}
}

// KeyAfter returns the key immediately after x.
// There will be no key less than the result and greater than x
func KeyAfter(x []byte) []byte {
	y := append([]byte{}, x...)
	return append(y, 0x00)
}

// PrefixSpan returns a Span that includes all keys with prefix p
func PrefixSpan(p []byte) Span {
	return Span{
		Begin: []byte(p),
		End:   PrefixEnd([]byte(p)),
	}
}

// PrefixEnd return the key > all the keys with prefix p, but < any other key
func PrefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	var end []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			end = make([]byte, i+1)
			copy(end, prefix)
			end[i] = c + 1
			break
		}
	}
	return end
}

// Literal is a stream literal, satisfying the Iterator interface.
// It can be constructed with a slice using NewLiteral
type Literal = streams.Slice[Entry]

func NewLiteral(xs []Entry) *Literal {
	return streams.NewSlice(xs, CopyEntry)
}
