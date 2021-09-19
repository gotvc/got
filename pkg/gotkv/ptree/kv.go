package ptree

import (
	"bytes"
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kv"
	"github.com/pkg/errors"
)

type (
	Span  = kv.Span
	Entry = kv.Entry
)

func MaxKey(ctx context.Context, s cadata.Store, x Root, under []byte) ([]byte, error) {
	op := gdat.NewOperator()
	sr := NewStreamReader(s, &op, []Index{rootToIndex(x)})
	ent, err := maxEntry(ctx, sr, under)
	if err != nil {
		return nil, err
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

func maxEntry(ctx context.Context, sr *StreamReader, under []byte) (ret *Entry, _ error) {
	// TODO: this can be more efficient using Peek
	var ent Entry
	for err := sr.Next(ctx, &ent); err != kv.EOS; err = sr.Next(ctx, &ent) {
		if err != nil {
			return nil, err
		}
		if under != nil && bytes.Compare(ent.Key, under) >= 0 {
			break
		}
		ent2 := ent.Clone()
		ret = &ent2
	}
	if ret == nil {
		return nil, kv.EOS
	}
	return ret, nil
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
		sr := NewStreamReader(s, &op, []Index{{Ref: x.Ref, First: x.First}})
		fmt.Printf("%sTREE NODE: %s %d\n", indent, x.Ref.CID.String(), x.Depth)
		if x.Depth == 0 {
			for {
				var ent Entry
				if err := sr.Next(ctx, &ent); err != nil {
					if err == kv.EOS {
						break
					}
					panic(err)
				}
				fmt.Printf("%s ENTRY key=%q value=%q\n", indent, string(ent.Key), string(ent.Value))
			}
		} else {
			for {
				var ent Entry
				if err := sr.Next(ctx, &ent); err != nil {
					if err == kv.EOS {
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
