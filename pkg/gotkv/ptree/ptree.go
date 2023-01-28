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

type Encoder interface {
	// WriteEntry encodes ent to dst and returns the number of bytes written or an error.
	// ErrOutOfRoom should be returned to indicate that the entry will not fit in the buffer.
	WriteEntry(dst []byte, ent kvstreams.Entry) (int, error)
	// EncodednLen returns the number of bytes that it would take to encode ent.
	// Calling EncodedLen should not affect the state of the Encoder.
	EncodedLen(ent kvstreams.Entry) int
	// Reset returns the encoder to it's orginally contructed state.
	Reset()
}

type Decoder interface {
	// ReadEntry parses an entry from src into ent.  It returns the number of bytes read or an error.a
	ReadEntry(src []byte, ent *kvstreams.Entry) (int, error)
	// PeekEntry is like ReadEntry except it should not affect the state of the Decoder
	PeekEntry(src []byte, ent *kvstreams.Entry) error
	// Reset returns the decoder to the original state for the star of a node.
	Reset(parentKey []byte)
}

// CompareFunc compares 2 keys
type CompareFunc = func(a, b []byte) int

const (
	MaxKeySize   = 4096
	MaxRefSize   = 256
	MaxTreeDepth = 255
)

// Root is the root of the tree
type Root[Ref any] struct {
	Ref   Ref    `json:"ref"`
	Depth uint8  `json:"depth"`
	First []byte `json:"first,omitempty"`
}

// ReadParams are parameters needed to read from a tree
type ReadParams[Ref any] struct {
	Store      Getter[Ref]
	NewDecoder func() Decoder
	ParseRef   func([]byte) (Ref, error)
	Compare    CompareFunc
}

// Copy copies all the entries from it to b.
func Copy[Ref any](ctx context.Context, b *Builder[Ref], it *Iterator[Ref]) error {
	var ent Entry
	for {
		level := min(b.syncLevel(), it.syncLevel())
		if err := it.next(ctx, level, &ent); err != nil {
			if err == kvstreams.EOS {
				return nil
			}
			return err
		}
		if err := b.put(ctx, level, ent.Key, ent.Value); err != nil {
			return err
		}
	}
}

// ListChildren returns the immediate children of root if any.
func ListChildren[Ref any](ctx context.Context, params ReadParams[Ref], root Root[Ref]) ([]Index[Ref], error) {
	if PointsToEntries(root) {
		return nil, fmt.Errorf("cannot list children of root with depth=%d", root.Depth)
	}
	sr := NewStreamReader(StreamReaderParams[Ref]{
		Store:   params.Store,
		Compare: params.Compare,
		Decoder: params.NewDecoder(),
		Indexes: []Index[Ref]{rootToIndex(root)},
	})
	var idxs []Index[Ref]
	var ent Entry
	for {
		if err := sr.Next(ctx, &ent); err != nil {
			if err == kvstreams.EOS {
				break
			}
			return nil, err
		}
		idx, err := entryToIndex(ent, params.ParseRef)
		if err != nil {
			return nil, err
		}
		idxs = append(idxs, idx)
	}
	return idxs, nil
}

// ListEntries returns a slice of all the entries pointed to by idx, directly.
// If idx points to other indexes directly, then ListEntries returns the entries for those indexes.
func ListEntries[Ref any](ctx context.Context, params ReadParams[Ref], idx Index[Ref]) ([]Entry, error) {
	sr := NewStreamReader(StreamReaderParams[Ref]{
		Store:   params.Store,
		Compare: params.Compare,
		Decoder: params.NewDecoder(),
		Indexes: []Index[Ref]{idx},
	})
	return kvstreams.Collect(ctx, sr)
}

// PointsToEntries returns true if root points to non-index Entries
func PointsToEntries[Ref any](root Root[Ref]) bool {
	return root.Depth == 0
}

// PointsToIndexes returns true if root points to indexes.
func PointsToIndexes[Ref any](root Root[Ref]) bool {
	return root.Depth > 0
}

func entryToIndex[Ref any](ent Entry, parseRef func([]byte) (Ref, error)) (Index[Ref], error) {
	ref, err := parseRef(ent.Value)
	if err != nil {
		return Index[Ref]{}, err
	}
	return Index[Ref]{
		First: append([]byte{}, ent.Key...),
		Ref:   ref,
	}, nil
}

func indexToEntry[Ref any](idx Index[Ref], marshalRef func([]byte, Ref) []byte) Entry {
	return Entry{Key: idx.First, Value: marshalRef(nil, idx.Ref)}
}

func indexToRoot[Ref any](idx Index[Ref], depth uint8) Root[Ref] {
	return Root[Ref]{
		Ref:   idx.Ref,
		First: idx.First,
		Depth: depth,
	}
}

func rootToIndex[Ref any](r Root[Ref]) Index[Ref] {
	return Index[Ref]{
		Ref:   r.Ref,
		First: r.First,
	}
}
