package gotnsop

import (
	"encoding/binary"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

type Entry struct {
	Name   string
	Volume blobcache.OID
	Rights blobcache.ActionSet

	// Aux is extra data associated with the volume.
	// This will be filled with branches.Info JSON.
	Aux []byte
}

func (e Entry) Key(buf []byte) []byte {
	buf = append(buf, e.Name...)
	return buf
}

func (e Entry) Value(buf []byte) []byte {
	buf = append(buf, e.Volume[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Rights))
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
	entry.Rights = blobcache.ActionSet(binary.LittleEndian.Uint64(value[16:24]))
	entry.Aux = value[24:]
	return entry, nil
}
