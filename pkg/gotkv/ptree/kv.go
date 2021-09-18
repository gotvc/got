package ptree

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/pkg/errors"
)

// Entry is a single entry in a stream/tree
type Entry struct {
	Key, Value []byte
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

func MaxKey(ctx context.Context, s cadata.Store, x Root, under []byte) ([]byte, error) {
	op := gdat.NewOperator()
	sr := NewStreamReader(s, &op, rootToIndex(x))
	var ent *Entry
	for {
		ent2, err := sr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if under != nil && bytes.Compare(ent2.Key, under) >= 0 {
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
	idx, err := entryToIndex(*ent)
	if err != nil {
		return nil, err
	}
	return MaxKey(ctx, s, indexToRoot(idx, x.Depth-1), under)
}

// AddPrefix returns a new version of root with the prefix prepended to all the keys
func AddPrefix(ctx context.Context, s cadata.Store, x Root, prefix []byte) (*Root, error) {
	var first []byte
	first = append(first, prefix...)
	first = append(first, x.First...)
	y := Root{
		First: first,
		Ref:   x.Ref,
		Depth: x.Depth,
	}
	return &y, nil
}

// RemovePrefix returns a new version of root with the prefix removed from all the keys
func RemovePrefix(ctx context.Context, s cadata.Store, x Root, prefix []byte) (*Root, error) {
	if yes, err := HasPrefix(ctx, s, x, prefix); err != nil {
		return nil, err
	} else if yes {
		return nil, errors.Errorf("tree does not have prefix %q", prefix)
	}
	y := Root{
		First: append([]byte{}, x.First[len(prefix):]...),
		Ref:   x.Ref,
		Depth: x.Depth,
	}
	return &y, nil
}

// HasPrefix returns true if the tree rooted at x only has keys which are prefixed with prefix
func HasPrefix(ctx context.Context, s cadata.Store, x Root, prefix []byte) (bool, error) {
	if !bytes.HasPrefix(x.First, prefix) {
		return false, nil
	}
	lastKey, err := MaxKey(ctx, s, x, nil)
	if err != nil {
		return false, err
	}
	if !bytes.HasPrefix(lastKey, prefix) {
		return false, nil
	}
	return true, nil
}

func DebugTree(s cadata.Store, x Root) {
	max := x.Depth
	op := gdat.NewOperator()
	var debugTree func(Root)
	debugTree = func(x Root) {
		indent := ""
		for i := 0; i < int(max-x.Depth); i++ {
			indent += "  "
		}
		ctx := context.TODO()
		sr := NewStreamReader(s, &op, Index{Ref: x.Ref, First: x.First})
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
				debugTree(Root{Ref: *ref, First: ent.Key, Depth: x.Depth - 1})
			}
		}
	}
	debugTree(x)
}
