package gotfs

import (
	"context"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

// Entry is an element of the filesystem.
// It contains a Key and Value
// The type can be determined using the Key.IsInfo() method
type Entry struct {
	Key
	Value
}

func (e *Entry) unmarshal(x gotkv.Entry) error {
	if err := e.Key.Unmarshal(x.Key); err != nil {
		return err
	}
	return e.Value.unmarshal(e.Key.IsInfo(), x.Value)
}

// Value is either an Info or an Extent
type Value struct {
	Info   Info
	Extent Extent
}

func (v *Value) unmarshal(isInfo bool, data []byte) error {
	if isInfo {
		return v.Info.unmarshal(data)
	} else {
		return v.Extent.UnmarshalBinary(data)
	}
}

func (v *Value) Marshal(isInfo bool, out []byte) []byte {
	if isInfo {
		return v.Info.Marshal(out)
	} else {
		data, err := v.Extent.MarshalBinary()
		if err != nil {
			panic(err)
		}
		return data
	}
}

var _ streams.Iterator[Entry] = &Iterator{}

// Iterate iterates over the metadata in a gotfs filesystem.
type Iterator struct {
	root Root
	s    stores.RO
	mdit *gotkv.Iterator
}

func (m *Machine) NewIterator(s stores.RO, root Root, subpath string) Iterator {
	subpath = cleanPath(subpath)
	span := SpanForPath(subpath)
	it := m.gotkv.NewIterator(s, root.toGotKV(), span)
	return Iterator{s: s, mdit: it}
}

func (it *Iterator) Next(ctx context.Context, dst []Entry) (int, error) {
	var kvent gotkv.Entry
	if err := streams.NextUnit(ctx, it.mdit, &kvent); err != nil {
		return 0, err
	}
	if err := dst[0].Key.Unmarshal(kvent.Key); err != nil {
		return 0, err
	}
	if dst[0].Key.IsInfo() {
		if err := dst[0].Info.Unmarshal(kvent.Value); err != nil {
			return 0, err
		}
	} else {
		if err := dst[0].Extent.UnmarshalBinary(kvent.Value); err != nil {
			return 0, err
		}
	}
	return 1, nil
}

// MetadataWriter writes metadata entries
// TODO: move dirpath checks to the metadata writer.
type MetadataWriter struct {
	m   *Machine
	s   stores.RW
	kvw gotkv.Builder
}

func (m *Machine) NewMetadataWriter(s stores.RW) MetadataWriter {
	return MetadataWriter{
		m:   m,
		s:   s,
		kvw: *m.gotkv.NewBuilder(s),
	}
}

// Push adds an entry to the metadata writer.
func (mdw *MetadataWriter) Push(ctx context.Context, ent Entry) error {
	return mdw.kvw.Put(ctx, ent.Key.Marshal(nil), ent.Value.Marshal(ent.IsInfo(), nil))
}

func (mdw *MetadataWriter) Finish(ctx context.Context) (Root, error) {
	kvr, err := mdw.kvw.Finish(ctx)
	if err != nil {
		return Root{}, err
	}
	r := newRoot(kvr)
	return *r, nil
}
