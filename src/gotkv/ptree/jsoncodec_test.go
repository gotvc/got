package ptree

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/state"
)

type Entry struct {
	Key, Value []byte
}

func (e Entry) String() string {
	return fmt.Sprintf("(%q => %q)", e.Key, e.Value)
}

func (e Entry) Clone() Entry {
	return Entry{
		Key:   append([]byte{}, e.Key...),
		Value: append([]byte{}, e.Value...),
	}
}

type JSONEncoder[T any] struct{}

func (enc *JSONEncoder[T]) Write(dst []byte, ent T) (int, error) {
	data, err := json.Marshal(ent)
	if err != nil {
		return 0, err
	}
	if len(data)+1 > len(dst) {
		return 0, ErrOutOfRoom
	}
	n := copy(dst, data)
	dst[n] = '\n'
	return n + 1, nil
}

func (enc *JSONEncoder[T]) EncodedLen(ent T) int {
	data, _ := json.Marshal(ent)
	return len(data) + 1
}

func (dec *JSONEncoder[T]) Reset() {}

type JSONDecoder[T any] struct{}

func (dec *JSONDecoder[T]) Read(src []byte, dst *T) (int, error) {
	return readJSONEntry(src, dst)
}

func (dec *JSONDecoder[T]) Peek(src []byte, dst *T) error {
	_, err := readJSONEntry(src, dst)
	return err
}

func (dec *JSONDecoder[T]) Reset(idx Index[T, blobcache.CID]) {}

type JSONIndexDecoder struct{}

func (dec *JSONIndexDecoder) Read(src []byte, dst *Index[Entry, blobcache.CID]) (int, error) {
	var ient indexEntry
	n, err := readJSONEntry(src, &ient)
	if err != nil {
		return 0, err
	}
	*dst = indexFromEntry(ient)
	return n, nil
}

func (dec *JSONIndexDecoder) Peek(src []byte, dst *Index[Entry, blobcache.CID]) error {
	var ient indexEntry
	_, err := readJSONEntry(src, &ient)
	if err != nil {
		return err
	}
	*dst = indexFromEntry(ient)
	return nil
}

func (dec *JSONIndexDecoder) Reset(idx Index[Entry, blobcache.CID]) {}

func readJSONEntry(src []byte, dst any) (int, error) {
	parts := bytes.SplitN(src, []byte{'\n'}, 2)
	if len(parts) < 2 {
		return 0, errors.New("could not parse next entry")
	}
	entryData := parts[0]
	if err := json.Unmarshal(entryData, dst); err != nil {
		return 0, err
	}
	return len(entryData) + 1, nil
}

type indexEntry struct {
	Ref       blobcache.CID
	First     []byte
	Last      []byte
	IsNatural bool
}

func newIndexEntry(idx Index[Entry, blobcache.CID]) indexEntry {
	lb, _ := idx.Span.LowerBound()
	ub, _ := idx.Span.UpperBound()
	return indexEntry{
		Ref:       idx.Ref,
		IsNatural: idx.IsNatural,
		First:     lb.Key,
		Last:      ub.Key,
	}
}

func indexFromEntry(x indexEntry) Index[Entry, blobcache.CID] {
	span := state.TotalSpan[Entry]().
		WithLowerIncl(Entry{Key: x.First}).
		WithUpperIncl(Entry{Key: x.Last})
	return Index[Entry, blobcache.CID]{
		Ref:       x.Ref,
		IsNatural: x.IsNatural,
		Span:      span,
	}
}

func NewEntryEncoder() Encoder[Entry] {
	return &JSONEncoder[Entry]{}
}

func NewIndexEncoder() IndexEncoder[Entry, blobcache.CID] {
	e := &JSONEncoder[indexEntry]{}
	return mapEncoder[indexEntry, Index[Entry, blobcache.CID]]{
		inner: e,
		fn: func(idx Index[Entry, blobcache.CID]) indexEntry {
			return newIndexEntry(idx)
		},
	}
}

func NewEntryDecoder() Decoder[Entry, blobcache.CID] {
	return &JSONDecoder[Entry]{}
}

func NewIndexDecoder() IndexDecoder[Entry, blobcache.CID] {
	return &JSONIndexDecoder{}
}

type mapEncoder[A, B any] struct {
	inner Encoder[A]
	fn    func(B) A
}

func (e mapEncoder[A, B]) Write(dst []byte, ent B) (int, error) {
	return e.inner.Write(dst, e.fn(ent))
}

func (e mapEncoder[A, B]) EncodedLen(x B) int {
	return e.inner.EncodedLen(e.fn(x))
}

func (e mapEncoder[A, B]) Reset() {
	e.inner.Reset()
}

func copyEntry(dst *Entry, src Entry) {
	dst.Key = append(dst.Key[:0], src.Key...)
	dst.Value = append(dst.Value[:0], src.Value...)
}

func compareEntries(a, b Entry) int {
	return bytes.Compare(a.Key, b.Key)
}
