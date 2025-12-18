package gotorgop

import (
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/sbe"
)

type VolumeEntry struct {
	Volume blobcache.OID
	// The hash of the secret shared amongst readers of the volume.
	// The double hash of the secret shared amongst writers of the volume.
	// There should never be more than two of these, or less than one.
	HashOfSecrets [][32]byte

	// Aux is extra data associated with the volume.
	// This will be filled with marks.Info JSON.
	Aux []byte
}

func (e VolumeEntry) Key(buf []byte) []byte {
	buf = append(buf, e.Volume[:]...)
	return buf
}

func (e VolumeEntry) Value(buf []byte) []byte {
	buf = sbe.AppendUint16(buf, uint16(len(e.HashOfSecrets)))
	for _, hash := range e.HashOfSecrets {
		buf = append(buf, hash[:]...)
	}
	buf = sbe.AppendLP(buf, e.Aux)
	return buf
}

func ParseVolumeEntry(key, value []byte) (*VolumeEntry, error) {
	var entry VolumeEntry
	if len(key) != 16 {
		return nil, fmt.Errorf("volume entry key too short: %d", len(key))
	}
	entry.Volume = blobcache.OID(key)

	numHashes, data, err := sbe.ReadUint16(value)
	if err != nil {
		return nil, err
	}
	entry.HashOfSecrets = make([][32]byte, numHashes)
	for i := range entry.HashOfSecrets {
		if len(data) < 32 {
			return nil, fmt.Errorf("volume entry value too short")
		}
		copy(entry.HashOfSecrets[i][:], data[:32])
		data = data[32:]
	}
	entry.Aux, data, err = sbe.ReadLP(data)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// VolumeAlias associates a name with a volume.
type VolumeAlias struct {
	Name string

	Volume blobcache.OID
}

func (e VolumeAlias) Key(buf []byte) []byte {
	buf = append(buf, e.Name...)
	return buf
}

func (e VolumeAlias) Value(buf []byte) []byte {
	buf = append(buf, e.Volume[:]...)
	return buf
}

func ParseVolumeAlias(key, value []byte) (VolumeAlias, error) {
	var entry VolumeAlias
	entry.Name = string(key)

	if len(value) < 16 {
		return VolumeAlias{}, fmt.Errorf("entry value too short")
	}
	entry.Volume = blobcache.OID(value[:16])
	return entry, nil
}
