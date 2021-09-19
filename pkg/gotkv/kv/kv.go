package kv

import (
	"bytes"
	"context"
	goerrors "errors"
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
var EOS = goerrors.New("end of stream")

type Iterator interface {
	Next(ctx context.Context, ent *Entry) error
	Seek(ctx context.Context, gteq []byte) error
	Peek(ctx context.Context, ent *Entry) error
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
// There will be no key less than the result and greater than x
func KeyAfter(x []byte) []byte {
	y := append([]byte{}, x...)
	return append(y, 0x00)
}
