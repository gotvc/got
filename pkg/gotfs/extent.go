package gotfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
)

// Extent is a reference to data using the gdat.Ref type.
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

func (e *Extent) Marshal() []byte {
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

func appendUint32(out []byte, x uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], x)
	return append(out, buf[:]...)
}

func splitExtentKey(k []byte) (p string, offset uint64, err error) {
	if len(k) < 9 {
		return "", 0, errors.Errorf("not extent key, too short: %q", k)
	}
	if k[len(k)-9] != 0x00 {
		return "", 0, errors.Errorf("not extent key, no NULL")
	}
	offset = binary.BigEndian.Uint64(k[len(k)-8:])
	p, err = parseInfoKey(k[:len(k)-9])
	if err != nil {
		return "", 0, err
	}
	return p, offset, nil
}

func makeExtentKey(p string, offset uint64) []byte {
	x := makeInfoKey(p)
	x = append(x, 0x00)
	x = appendUint64(x, offset)
	return x
}

func isExtentKey(x []byte) bool {
	return len(x) >= 9 && bytes.Index(x, []byte{0x00}) == len(x)-9
}

func fileSpanEnd(p string) []byte {
	mk := makeInfoKey(p)
	return gotkv.PrefixEnd(mk)
}

func appendUint64(buf []byte, n uint64) []byte {
	nbytes := [8]byte{}
	binary.BigEndian.PutUint64(nbytes[:], n)
	return append(buf, nbytes[:]...)
}

func (o *Operator) getExtentF(ctx context.Context, s Store, ext *Extent, fn func(data []byte) error) error {
	return o.rawOp.GetF(ctx, s, ext.Ref, func(data []byte) error {
		if int(ext.Offset) >= len(data) {
			return errors.Errorf("extent offset %d is >= len(data) %d", ext.Offset, len(data))
		}
		data = data[ext.Offset : ext.Offset+ext.Length]
		return fn(data)
	})
}

func (o *Operator) postExtent(ctx context.Context, s Store, data []byte) (*Extent, error) {
	ref, err := o.rawOp.Post(ctx, s, data)
	if err != nil {
		return nil, err
	}
	ext := &Extent{
		Ref:    *ref,
		Offset: 0,
		Length: uint32(len(data)),
	}
	return ext, nil
}
