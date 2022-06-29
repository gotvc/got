package gotlob

import (
	"encoding/binary"
	"fmt"

	"github.com/gotvc/got/pkg/gdat"
)

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
