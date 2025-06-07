package kvstreams

import (
	"context"
	"fmt"

	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state"
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
type Iterator interface {
	streams.Iterator[Entry]
	streams.Peekable[Entry]

	Seek(ctx context.Context, gteq []byte) error
}

func Peek(ctx context.Context, it Iterator) (*Entry, error) {
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
type Span = state.ByteSpan

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
