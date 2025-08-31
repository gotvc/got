package gotns

import (
	"context"
	"encoding/binary"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

// NewEntry creates a new entry with the provided information
// and produces KEMs for each group with access to the entry.
func (m *Machine) NewEntry(ctx context.Context, name string, rights blobcache.ActionSet, volume blobcache.OID, secret *[32]byte) (Entry, error) {
	entry := Entry{
		Name:   name,
		Rights: rights,
		Volume: volume,
	}
	return entry, nil
}

type Entry struct {
	Name   string
	Volume blobcache.OID
	Rights blobcache.ActionSet

	// Aux is extra data associated with the volume.
	Aux []byte
}

func (e Entry) Key(buf []byte) []byte {
	buf = append(buf, e.Name...)
	return buf
}

func (e Entry) Value(buf []byte) []byte {
	buf = append(buf, e.Volume[:]...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(e.Rights))
	buf = append(buf, e.Aux...)
	return buf
}

func ParseEntry(key, value []byte) (Entry, error) {
	var entry Entry
	entry.Name = string(key)

	if len(value) < 16+8 {
		return Entry{}, fmt.Errorf("entry value too short")
	}
	entry.Volume = blobcache.OID(value[:16])
	entry.Rights = blobcache.ActionSet(binary.BigEndian.Uint64(value[16:24]))
	entry.Aux = value[24:]
	return entry, nil
}

func (m *Machine) GetEntry(ctx context.Context, s stores.Reading, State State, name []byte) (*Entry, error) {
	val, err := m.gotkv.Get(ctx, s, State.Branches, name)
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	entry, err := ParseEntry(name, val)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

func (m *Machine) PutEntry(ctx context.Context, s stores.RW, State State, entry Entry) (*State, error) {
	mut := PutEntry(entry)
	entsState, err := m.gotkv.Mutate(ctx, s, State.Branches, mut)
	if err != nil {
		return nil, err
	}
	State.Branches = *entsState
	return &State, nil
}

func (m *Machine) DeleteEntry(ctx context.Context, s stores.RW, State State, name string) (*State, error) {
	entsState, err := m.gotkv.Delete(ctx, s, State.Branches, []byte(name))
	if err != nil {
		return nil, err
	}
	State.Branches = *entsState
	return &State, nil
}

func PutEntry(entry Entry) gotkv.Mutation {
	k := entry.Key(nil)
	return gotkv.Mutation{
		Span: gotkv.SingleKeySpan(k),
		Entries: []gotkv.Entry{
			{
				Key:   entry.Key(nil),
				Value: entry.Value(nil),
			},
		},
	}
}

func (m *Machine) ListEntries(ctx context.Context, s stores.Reading, State State, limit int) ([]Entry, error) {
	span := gotkv.PrefixSpan([]byte(""))
	it := m.gotkv.NewIterator(s, State.Branches, span)
	var ents []Entry
	for {
		ent, err := streams.Next(ctx, it)
		if err != nil {
			if streams.IsEOS(err) {
				break
			}
			return nil, err
		}

		entry, err := ParseEntry(ent.Key, ent.Value)
		if err != nil {
			return nil, err
		}
		ents = append(ents, entry)
		if limit > 0 && len(ents) >= limit {
			break
		}
	}
	return ents, nil
}
