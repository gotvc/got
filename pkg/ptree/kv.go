package ptree

import (
	"bytes"
	"context"
	"io"
	"log"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/refs"
)

// Entry is a single entry in a stream/tree
type Entry struct {
	Key, Value []byte
}

// Root it the root of the tree
type Root struct {
	Ref   Ref
	Depth uint
}

// A span of keys [First, Last)
type Span struct {
	First, Last []byte
}

func TotalSpan() Span {
	return Span{}
}

func SingleItemSpan(x []byte) Span {
	return Span{
		First: x,
		Last:  KeyAfter(x),
	}
}

// LessThan returns true if every key in the Span is below key
func (s Span) LessThan(key []byte) bool {
	return s.Last != nil && bytes.Compare(s.Last, key) <= 0
}

// GreaterThan returns true if every key in the span is greater than k
func (s Span) GreaterThan(k []byte) bool {
	return s.First != nil && bytes.Compare(s.First, k) > 0
}

func (s Span) Contains(k []byte) bool {
	return !s.GreaterThan(k) && !s.LessThan(k)
}

func KeyAfter(x []byte) []byte {
	y := append([]byte{}, x...)
	return append(y, 0x00)
}

func put(k, v []byte) entryMutator {
	return func(ent *Entry) ([]Entry, error) {
		return []Entry{{Key: k, Value: v}}, nil
	}
}

func MaxKey(ctx context.Context, s cadata.Store, x Root, under []byte) ([]byte, error) {
	sr := NewStreamReader(s, x.Ref)
	var ent *Entry
	for {
		ent2, err := sr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if bytes.Compare(ent2.Key, under) >= 0 {
			break
		}
		ent = ent2
	}
	if ent == nil {
		return nil, io.EOF
	}
	if x.Depth == 0 {
		return ent.Key, nil
	}
	ref, err := refs.ParseRef(ent.Value)
	if err != nil {
		return nil, err
	}
	return MaxKey(ctx, s, Root{Ref: ref, Depth: x.Depth - 1}, under)
}

// func maxKeyFromIterator(ctx context.Context, it StreamIterator, under []byte) ([]byte, error) {
// 	var ent *Entry
// 	for {
// 		ent2, err := it.Next(ctx)
// 		if err != nil {
// 			if err == io.EOF {
// 				break
// 			}
// 			return nil, err
// 		}
// 		if bytes.Compare(ent2.Key, under) >= 0 {
// 			break
// 		}
// 		ent = ent2
// 	}
// 	if ent == nil {
// 		return nil, io.EOF
// 	}
// 	return ent.Key, nil
// }

func DebufRef(s cadata.Store, x Ref) {
	ctx := context.TODO()
	sr := NewStreamReader(s, x)
	log.Println("DUMP ref:", x.CID.String())
	for {
		ent, err := sr.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		log.Println("entry:", string(ent.Key), "->", ent.Value)
	}
}
