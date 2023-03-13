package ptree

import (
	"context"
	"errors"
	"fmt"

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
	// WriteEntry encodes ent to dst and returns the number of bytes written or an error.
	// ErrOutOfRoom should be returned to indicate that the entry will not fit in the buffer.
	WriteEntry(dst []byte, ent T) (int, error)
	// EncodednLen returns the number of bytes that it would take to encode ent.
	// Calling EncodedLen should not affect the state of the Encoder.
	EncodedLen(ent T) int
	// Reset returns the encoder to it's orginally contructed state.
	Reset()
}

type Decoder[T, Ref any] interface {
	// ReadEntry parses an entry from src into ent.  It returns the number of bytes read or an error.
	ReadEntry(src []byte, ent *T) (int, error)
	// PeekEntry is like ReadEntry except it should not affect the state of the Decoder
	PeekEntry(src []byte, ent *T) error
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
type Root[T, Ref any] struct {
	Ref   Ref   `json:"ref"`
	Depth uint8 `json:"depth"`
	First T     `json:"first,omitempty"`
}

// ReadParams are parameters needed to read from a tree
type ReadParams[T, Ref any] struct {
	Store        Getter[Ref]
	NewDecoder   func() Decoder[T, Ref]
	ParseRef     func([]byte) (Ref, error)
	Compare      CompareFunc[T]
	ConvertEntry func(T) (Index[T, Ref], error)
}

// Copy copies all the entries from it to b.
func Copy[T, Ref any](ctx context.Context, b *Builder[T, Ref], it *Iterator[T, Ref]) error {
	var ent T
	for {
		level := min(b.syncLevel(), it.syncLevel())
		if err := it.next(ctx, level, &ent); err != nil {
			if err == kvstreams.EOS {
				return nil
			}
			return err
		}
		if err := b.put(ctx, level, ent); err != nil {
			return err
		}
	}
}

// ListChildren returns the immediate children of root if any.
func ListChildren[T, Ref any](ctx context.Context, params ReadParams[T, Ref], root Root[T, Ref]) ([]Index[T, Ref], error) {
	if PointsToEntries(root) {
		return nil, fmt.Errorf("cannot list children of root with depth=%d", root.Depth)
	}
	sr := NewStreamReader(StreamReaderParams[T, Ref]{
		Store:   params.Store,
		Compare: params.Compare,
		Decoder: params.NewDecoder(),
		Indexes: []Index[T, Ref]{rootToIndex(root)},
	})
	var idxs []Index[T, Ref]
	for {
		var ent T
		if err := sr.Next(ctx, &ent); err != nil {
			if err == EOS {
				break
			}
			return nil, err
		}
		idx, err := params.ConvertEntry(ent)
		if err != nil {
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
		Indexes: []Index[T, Ref]{idx},
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
		Ref:   idx.Ref,
		First: idx.First,
		Depth: depth,
	}
}

func rootToIndex[T, Ref any](r Root[T, Ref]) Index[T, Ref] {
	return Index[T, Ref]{
		Ref:   r.Ref,
		First: r.First,
	}
}
