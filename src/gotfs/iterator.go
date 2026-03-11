package gotfs

import (
	"context"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

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

var _ streams.Iterator[Entry] = &Iterator{}

// Iterate iterates over the metadata in a gotfs filesystem.
type Iterator struct {
	root Root
	s    stores.Reading
	mdit *gotkv.Iterator
}

func (m *Machine) NewIterator(s stores.Reading, root Root, span Span) Iterator {
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
		if err := dst[0].Info.Unmarshal(kvent.Value); err != nil {
			return 0, err
		}
	}
	return 1, nil
}
