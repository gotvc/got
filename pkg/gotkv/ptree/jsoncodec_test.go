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

type JSONEncoder struct{}

func NewJSONEncoder() Encoder[Entry] {
	return &JSONEncoder{}
}

func (enc *JSONEncoder) WriteEntry(dst []byte, ent Entry) (int, error) {
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

func (enc *JSONEncoder) EncodedLen(ent Entry) int {
	data, _ := json.Marshal(ent)
	return len(data) + 1
}

func (dec *JSONEncoder) Reset() {}

type JSONDecoder struct{}

func NewJSONDecoder() Decoder[Entry, cadata.ID] {
	return &JSONDecoder{}
}

func (dec *JSONDecoder) ReadEntry(src []byte, ent *Entry) (int, error) {
	parts := bytes.SplitN(src, []byte{'\n'}, 2)
	if len(parts) < 2 {
		return 0, errors.New("could not parse next entry")
	}
	entryData := parts[0]
	if err := json.Unmarshal(entryData, ent); err != nil {
		return 0, err
	}
	return len(entryData) + 1, nil
}

func (dec *JSONDecoder) PeekEntry(src []byte, ent *Entry) error {
	_, err := dec.ReadEntry(src, ent)
	return err
}

func (dec *JSONDecoder) Reset(idx Index[Entry, cadata.ID]) {}
