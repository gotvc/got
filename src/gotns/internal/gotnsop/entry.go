package gotnsop

import (
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

type VolumeEntry struct {
	Volume blobcache.OID
	// The hash of the secret shared amongst readers of the volume.
	// The double hash of the secret shared amongst writers of the volume.
	HashOfSecret [32]byte
}

func (e VolumeEntry) Key(buf []byte) []byte {
	buf = append(buf, e.Volume[:]...)
	return buf
}

func (e VolumeEntry) Value(buf []byte) []byte {
	buf = append(buf, e.HashOfSecret[:]...)
	return buf
}

func ParseVolumeEntry(key, value []byte) (*VolumeEntry, error) {
	if len(key) != 16 {
		return nil, fmt.Errorf("volume entry key too short: %d", len(key))
	}
	if len(value) != 32 {
		return nil, fmt.Errorf("volume entry value too short: %d", len(value))
	}
	var entry VolumeEntry
	entry.Volume = blobcache.OID(key)
	entry.HashOfSecret = [32]byte(value)
	return &entry, nil
}

type BranchEntry struct {
	Name string

	Volume blobcache.OID
	// Aux is extra data associated with the volume.
	// This will be filled with branches.Info JSON.
	Aux []byte
}

func (e BranchEntry) Key(buf []byte) []byte {
	buf = append(buf, e.Name...)
	return buf
}

func (e BranchEntry) Value(buf []byte) []byte {
	buf = append(buf, e.Volume[:]...)
	buf = append(buf, e.Aux...)
	return buf
}

func ParseBranchEntry(key, value []byte) (BranchEntry, error) {
	var entry BranchEntry
	entry.Name = string(key)

	if len(value) < 16 {
		return BranchEntry{}, fmt.Errorf("entry value too short")
	}
	entry.Volume = blobcache.OID(value[:16])
	entry.Aux = value[16:]
	return entry, nil
}
