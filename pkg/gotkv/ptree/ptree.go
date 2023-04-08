package ptree

import (
	"context"
	"errors"
	"fmt"

	"github.com/brendoncarroll/go-state"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

// Getter is used to retrieve nodes from storage by Ref
type Getter[Ref any] interface {
	// Get fills buf with data at ref, or returns an error.
	// Get will always return an n <= MaxSize()
	Get(ctx context.Context, ref Ref, buf []byte) (n int, err error)
	// MaxSize returns the maximum amount of bytes that could be stored at a ref.
	MaxSize() int
}

// Poster is used to store nodes in storage and retrieve a Ref
type Poster[Ref any] interface {
	// Posts stores the data and returns a Ref for retrieving it.
	Post(ctx context.Context, data []byte) (Ref, error)
	// MaxSize is the maximum amount of data that can be Posted in bytes
	MaxSize() int
}

type Store[Ref any] interface {
	Getter[Ref]
	Poster[Ref]
}

// ErrOutOfRoom is returned when an encoder does not have enough room to write an entry.
var ErrOutOfRoom = errors.New("out of room")

var EOS = kvstreams.EOS

type Encoder[T any] interface {
	// Write encodes ent to dst and returns the number of bytes written or an error.
	// ErrOutOfRoom should be returned to indicate that the entry will not fit in the buffer.
	Write(dst []byte, ent T) (int, error)
	// EncodednLen returns the number of bytes that it would take to encode ent.
	// Calling EncodedLen should not affect the state of the Encoder.
	EncodedLen(ent T) int
	// Reset returns the encoder to it's orginally contructed state.
	Reset()
}

type Decoder[T, Ref any] interface {
	// ReadEntry parses an entry from src into ent.  It returns the number of bytes read or an error.
	Read(src []byte, ent *T) (int, error)
	// PeekEntry is like ReadEntry except it should not affect the state of the Decoder
	Peek(src []byte, ent *T) error
	// Reset returns the decoder to the original state for the star of a node.
	Reset(parent Index[T, Ref])
}

// CompareFunc compares 2 keys
type CompareFunc[T any] func(a, b T) int

const (
	MaxKeySize   = 4096
	MaxRefSize   = 256
	MaxTreeDepth = 255
)

// Root is the root of the tree
// It contains the same information as an Index, plus the depth of the tree
type Root[T, Ref any] struct {
	Index[T, Ref]
	Depth uint8
}

func (r *Root[T, Ref]) Index2() Index[Index[T, Ref], Ref] {
	return Index[Index[T, Ref], Ref]{
		Ref:   r.Ref,
		First: Just(Index[T, Ref]{First: r.First}),
		Last:  Just(Index[T, Ref]{Last: r.Last}),
	}
}

// ReadParams are parameters needed to read from a tree
type ReadParams[T, Ref any] struct {
	Store           Getter[Ref]
	NewDecoder      func() Decoder[T, Ref]
	NewIndexDecoder func() Decoder[Index[T, Ref], Ref]
	Compare         CompareFunc[T]
}

// Copy copies all the entries from it to b.
func Copy[T, Ref any](ctx context.Context, b *Builder[T, Ref], it *Iterator[T, Ref]) error {
	var ent T
	var idx Index[T, Ref]
	x := dual[T, Ref]{
		Entry: &ent,
		Index: &idx,
	}
	for {
		level := min(b.syncLevel(), it.syncLevel())
		if err := it.next(ctx, level, x); err != nil {
			if err == kvstreams.EOS {
				return nil
			}
			return err
		}
		if err := b.put(ctx, level, x); err != nil {
			return err
		}
	}
}

// ListChildren returns the immediate children of root if any.
func ListChildren[T, Ref any](ctx context.Context, params ReadParams[T, Ref], root Root[T, Ref]) ([]Index[T, Ref], error) {
	if PointsToEntries(root) {
		return nil, fmt.Errorf("cannot list children of root with depth=%d", root.Depth)
	}
	sr := NewStreamReader(StreamReaderParams[Index[T, Ref], Ref]{
		Store:     params.Store,
		Compare:   upgradeCompare[T, Ref](params.Compare),
		Decoder:   params.NewIndexDecoder(),
		NextIndex: NextIndexFromSlice([]Index[Index[T, Ref], Ref]{root.Index2()}),
	})
	var idxs []Index[T, Ref]
	for {
		var idx Index[T, Ref]
		if err := sr.Next(ctx, &idx); err != nil {
			if err == EOS {
				break
			}
			return nil, err
		}
		idxs = append(idxs, idx)
	}
	return idxs, nil
}

// ListEntries returns a slice of all the entries pointed to by idx, directly.
// If idx points to other indexes directly, then ListEntries returns the entries for those indexes.
func ListEntries[T, Ref any](ctx context.Context, params ReadParams[T, Ref], idx Index[T, Ref]) ([]T, error) {
	sr := NewStreamReader(StreamReaderParams[T, Ref]{
		Store:   params.Store,
		Compare: params.Compare,
		Decoder: params.NewDecoder(),
	})
	var ret []T
	for {
		var ent T
		if err := sr.Next(ctx, &ent); err != nil {
			if errors.Is(err, EOS) {
				break
			}
			return nil, err
		}
		ret = append(ret, ent)
	}
	return ret, nil
}

// PointsToEntries returns true if root points to non-index Entries
func PointsToEntries[T, Ref any](root Root[T, Ref]) bool {
	return root.Depth == 0
}

// PointsToIndexes returns true if root points to indexes.
func PointsToIndexes[T, Ref any](root Root[T, Ref]) bool {
	return root.Depth > 0
}

// func entryToIndex[Ref any](ent Entry, parseRef func([]byte) (Ref, error)) (Index[Ref], error) {
// 	ref, err := parseRef(ent.Value)
// 	if err != nil {
// 		return Index[Ref]{}, err
// 	}
// 	return Index[Ref]{
// 		First: append([]byte{}, ent.Key...),
// 		Ref:   ref,
// 	}, nil
// }

// func indexToEntry[T, Ref any](idx Index[T, Ref], marshalRef func([]byte, Ref) []byte) Entry {
// 	return Entry{Key: idx.First, Value: marshalRef(nil, idx.Ref)}
// }

func indexToRoot[T, Ref any](idx Index[T, Ref], depth uint8) Root[T, Ref] {
	return Root[T, Ref]{
		Index: idx,
		Depth: depth,
	}
}

// compareSpans returns
//
//	 1: If all elements in a are greater than all the elements of b
//	-1: If all elements in a are less than all the elements of b
//	 0: If any elements in a are also in b
func compareSpans[T any](a, b state.Span[T], fn func(a, b T) int) int {
	return 0
}

// cloneSpan makes a clone of a span, using copy to make copies of the bounds
func cloneSpan[T any](x state.Span[T], copy func(dst *T, src T)) (ret state.Span[T]) {
	if lb, ok := x.LowerBound(); ok {
		var bound T
		copy(&bound, lb)
		if x.IncludesLower() {
			ret = ret.WithLowerIncl(bound)
		} else {
			ret = ret.WithLowerExcl(bound)
		}
	}
	if ub, ok := x.UpperBound(); ok {
		var bound T
		copy(&bound, ub)
		if x.IncludesUpper() {
			ret = ret.WithUpperIncl(bound)
		} else {
			ret = ret.WithUpperExcl(bound)
		}
	}
	return ret
}

// dual is either an Entry or an Index
type dual[T, Ref any] struct {
	Entry *T
	Index *Index[T, Ref]
}

func upgradeCompare[T, Ref any](cmp func(a, b T) int) func(a, b Index[T, Ref]) int {
	return func(a, b Index[T, Ref]) int {
		if a.Last.Ok && b.First.Ok {
			if cmp(a.Last.Value, b.First.Value) < 0 {
				return -1
			}
		}
		if a.First.Ok && b.Last.Ok {
			if cmp(a.First.Value, b.Last.Value) > 0 {
				return 1
			}
		}
		return 0
	}
}

func upgradeCopy[T, Ref any](copy func(dst *T, src T)) func(dst *Index[T, Ref], src Index[T, Ref]) {
	return func(dst *Index[T, Ref], src Index[T, Ref]) {
		dst.Ref = src.Ref
		copyMaybe(&dst.First, src.First, copy)
		copyMaybe(&dst.Last, src.Last, copy)
	}
}

func copyMaybe[T any](dst *Maybe[T], src Maybe[T], copy func(dst *T, src T)) {
	if !src.Ok {
		return
	}
	dst.Ok = true
	copy(&dst.Value, src.Value)
}

func flattenIndex[T, Ref any](x Index[Index[T, Ref], Ref]) Index[T, Ref] {
	var first Maybe[T]
	if x.First.Ok {
		first = x.First.Value.First
	}
	var last Maybe[T]
	if x.Last.Ok {
		last = x.Last.Value.Last
	}
	return Index[T, Ref]{
		Ref:   x.Ref,
		First: first,
		Last:  last,
	}
}
