package ptree

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
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

// A span of keys [Start, End)
// If you want to include a specific end key, use the KeyAfter function.
// nil is interpretted as no bound, not as a 0 length key.  This behaviour is only releveant for End.
type Span struct {
	Start, End []byte
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
	sr := NewStreamReader(s, Index{Ref: x.Ref})
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
	ref, err := gdat.ParseRef(ent.Value)
	if err != nil {
		return nil, err
	}
	return MaxKey(ctx, s, Root{Ref: *ref, Depth: x.Depth - 1}, under)
}

func DebugRef(s cadata.Store, x Ref) {
	ctx := context.TODO()
	sr := NewStreamReader(s, Index{Ref: x})
	log.Println("DUMP ref:", x.CID.String())
	for {
		ent, err := sr.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		log.Println("entry:", string(ent.Key), "->", string(ent.Value))
	}
}

func DebugTree(s cadata.Store, x Root) {
	max := x.Depth

	var debugTree func(Root)
	debugTree = func(x Root) {
		indent := ""
		for i := 0; i < int(max-x.Depth); i++ {
			indent += "  "
		}
		ctx := context.TODO()
		sr := NewStreamReader(s, Index{Ref: x.Ref})
		fmt.Printf("%sTREE NODE: %s %d\n", indent, x.Ref.CID.String(), x.Depth)
		if x.Depth == 0 {
			for {
				ent, err := sr.Next(ctx)
				if err != nil {
					if err == io.EOF {
						break
					}
					panic(err)
				}
				fmt.Printf("%s ENTRY key=%q value=%q\n", indent, string(ent.Key), string(ent.Value))
			}
		} else {
			for {
				ent, err := sr.Next(ctx)
				if err != nil {
					if err == io.EOF {
						break
					}
					panic(err)
				}
				ref, err := gdat.ParseRef(ent.Value)
				if err != nil {
					panic(err)
				}
				fmt.Printf("%s INDEX first=%q -> ref=%s\n", indent, string(ent.Key), ref.CID.String())
				debugTree(Root{Ref: *ref, Depth: x.Depth - 1})
			}
		}
	}
	debugTree(x)
}
