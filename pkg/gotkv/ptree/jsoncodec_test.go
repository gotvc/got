package ptree

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/brendoncarroll/go-state/cadata"
)

type Entry struct {
	Key, Value []byte
}

func (e Entry) Clone() Entry {
	return Entry{Key: e.Key, Value: e.Value}
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

func (dec *JSONDecoder[T]) Peek(src []byte, dst *T) error {
	_, err := dec.Read(src, dst)
	return err
}

func (dec *JSONDecoder[T]) Reset(idx Index[T, cadata.ID]) {}

func NewEntryEncoder() Encoder[Entry] {
	return &JSONEncoder[Entry]{}
}

func NewIndexEncoder() Encoder[Index[Entry, cadata.ID]] {
	return &JSONEncoder[Index[Entry, cadata.ID]]{}
}

func NewEntryDecoder() Decoder[Entry, cadata.ID] {
	return &JSONDecoder[Entry]{}
}

func NewIndexDecoder() Decoder[Index[Entry, cadata.ID], cadata.ID] {
	return &JSONDecoder[Index[Entry, cadata.ID]]{}
}
