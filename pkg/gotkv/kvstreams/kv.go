package kvstreams

import (
	"bytes"
	"context"
	"fmt"
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

// EOS signals the end of a stream
var EOS = fmt.Errorf("end of stream")

// Iterator iterates over entries
//
// e.g.
// if err := it.Seek(ctx, key); err != nil {
//   return err
// }
// var ent Entry
// for err := it.Next(ctx, &ent); err != EOS; err = it.Next(ctx, &ent) {
//   if err != nil {
//	   return err
//   }
//   // use ent here. ent will be valid until the next call to it.Next
// }
type Iterator interface {
	Next(ctx context.Context, ent *Entry) error
	Seek(ctx context.Context, gteq []byte) error
	Peek(ctx context.Context, ent *Entry) error
}

func Peek(ctx context.Context, it Iterator) (*Entry, error) {
	var ent Entry
	if err := it.Peek(ctx, &ent); err != nil {
		return nil, err
	}
	return &ent, nil
}

func ForEach(ctx context.Context, it Iterator, fn func(ent Entry) error) error {
	var ent Entry
	for err := it.Next(ctx, &ent); err != EOS; err = it.Next(ctx, &ent) {
		if err != nil {
			return err
		}
		if err := fn(ent); err != nil {
			return err
		}
	}
	return nil
}

// A span of keys [Start, End)
// If you want to include a specific end key, use the KeyAfter function.
// nil is interpretted as no bound, not as a 0 length key.  This behaviour is only releveant for End.
type Span struct {
	Start, End []byte
}

func (s Span) String() string {
	return fmt.Sprintf("[%q, %q)", s.Start, s.End)
}

func TotalSpan() Span {
	return Span{}
}

func SingleItemSpan(x []byte) Span {
	return Span{
		Start: x,
		End:   KeyAfter(x),
	}
}

// LessThan returns true if every key in the Span is below key
func (s Span) LessThan(key []byte) bool {
	return s.End != nil && bytes.Compare(s.End, key) <= 0
}

// GreaterThan returns true if every key in the span is greater than k
func (s Span) GreaterThan(k []byte) bool {
	return s.Start != nil && bytes.Compare(s.Start, k) > 0
}

func (s Span) Contains(k []byte) bool {
	return !s.GreaterThan(k) && !s.LessThan(k)
}

func (s Span) Clone() Span {
	var start, end []byte
	if s.Start != nil {
		start = append([]byte{}, s.Start...)
	}
	if s.End != nil {
		end = append([]byte{}, s.End...)
	}
	return Span{
		Start: start,
		End:   end,
	}
}

// KeyAfter returns the key immediately after x.
func KeyAfter(x []byte) []byte {
	y := append([]byte{}, x...)
	return append(y, 0x00)
}

// PrefixSpan returns a Span that includes all keys with prefix p
func PrefixSpan(p []byte) Span {
	return Span{
		Start: []byte(p),
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
