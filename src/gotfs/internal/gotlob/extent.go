package gotlob

import (
	"encoding/binary"
	"fmt"

	"github.com/gotvc/got/src/gdat"
)

// Extent is a reference to data using the gdat.Ref type.
type Extent struct {
	// Ref points to the blob that contains the Extent data
	Ref gdat.Ref
	// Offset is where the extent starts within a Blob
	Offset uint32
	// Length is the length of the Extent, starting at the offset.
	Length uint32
}

func (e *Extent) MarshalBinary() ([]byte, error) {
	var buf []byte
	buf = gdat.AppendRef(buf, e.Ref)
	buf = appendUint32(buf, e.Offset)
	buf = appendUint32(buf, e.Length)
	return buf, nil
}

func (e *Extent) UnmarshalBinary(data []byte) error {
	if len(data) < 8+64 {
		return fmt.Errorf("too short to be extent: %q", data)
	}
	ref, err := gdat.ParseRef(data[:len(data)-8])
	if err != nil {
		return err
	}
	e.Offset = binary.BigEndian.Uint32(data[len(data)-8:])
	e.Length = binary.BigEndian.Uint32(data[len(data)-4:])
	e.Ref = ref
	return nil
}

func MarshalExtent(e *Extent) []byte {
	data, err := e.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return data
}

func ParseExtent(x []byte) (*Extent, error) {
	var e Extent
	if err := e.UnmarshalBinary(x); err != nil {
		return nil, err
	}
	return &e, nil
}

func ParseExtentKey(x []byte) ([]byte, uint64, error) {
	if len(x) < 8 {
		return nil, 0, fmt.Errorf("key too short to contain suffix len=%d", len(x))
	}
	splitI := len(x) - 8
	k := x[:splitI]
	offset := binary.BigEndian.Uint64(x[splitI:])
	return k, offset, nil
}

func appendKey(out []byte, prefix []byte, offset uint64) []byte {
	out = append(out, prefix...)
	out = appendUint64(out, offset)
	return out
}

func appendUint64(out []byte, x uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], x)
	return append(out, buf[:]...)
}

func appendUint32(out []byte, x uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], x)
	return append(out, buf[:]...)
}

func checkExtentBounds(ext *Extent, n int) error {
	if n < int(ext.Offset) || n < int(ext.Offset+ext.Length) {
		return fmt.Errorf("extent data too short len=%d offset=%d length=%d", n, ext.Offset, ext.Length)
	}
	return nil
}
