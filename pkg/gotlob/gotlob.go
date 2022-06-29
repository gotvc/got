package gotlob

import (
	"encoding/binary"
	"fmt"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
)

type (
	Ref   = gotkv.Ref
	Store = gotkv.Store
	Root  = gotkv.Root
)

const (
	DefaultMaxBlobSize = 1 << 21

	DefaultMinBlobSizeData     = 1 << 12
	DefaultAverageBlobSizeData = 1 << 20

	DefaultAverageBlobSizeKV = 1 << 13
)

type Span struct {
	span gotkv.Span
}

type Segment struct {
	Root Root
	Span Span
}

func (s Segment) String() string {
	return fmt.Sprintf("{ %v : %v}", s.Span, s.Root.Ref.CID)
}

type Extent struct {
	Ref    gdat.Ref
	Offset uint32
	Length uint32
}

func (e *Extent) MarshalBinary() ([]byte, error) {
	var buf []byte
	buf = append(buf, gdat.MarshalRef(e.Ref)...)
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
	e.Ref = *ref
	return nil
}

func marshalExtent(e *Extent) []byte {
	data, err := e.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return data
}

func parseExtent(x []byte) (*Extent, error) {
	var e Extent
	if err := e.UnmarshalBinary(x); err != nil {
		return nil, err
	}
	return &e, nil
}

func parseKey(x []byte) ([]byte, uint8, uint64, error) {
	if len(x) < 9 {
		return nil, 0, 0, fmt.Errorf("key too short to contain suffix len=%d", len(x))
	}
	k := x[:len(x)-9]
	sid := uint8(x[len(x)-9])
	offset := binary.BigEndian.Uint64(x[len(x)-9:])
	return k, sid, offset, nil
}

func makeStreamID(inline bool, id int8) uint8 {
	if id < 0 {
		panic(id)
	}
	ret := uint8(id)
	if !inline {
		ret |= 0x80
	}
	return ret
}

func appendKey(out []byte, k []byte, sid uint8, offset uint64) []byte {
	out = append(out, k...)
	out = append(out, 0xFF)
	out = appendUint64(out, offset)
	return out
}

func appendUint64(out []byte, x uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], x)
	return append(out, buf[:]...)
}

func appendUint32(out []byte, x uint32) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[:], x)
	return append(out, buf[:]...)
}

func appendVarint32(out []byte, x uint32) []byte {
	var buf [binary.MaxVarintLen32]byte
	binary.PutUvarint(buf[:], uint64(x))
	return append(out, buf[:]...)
}
