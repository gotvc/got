package gotfs

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

func (p *Extent) marshal() []byte {
	data, err := proto.Marshal(p)
	if err != nil {
		panic(err)
	}
	return data
}

func parseExtent(data []byte) (*Extent, error) {
	p := &Extent{}
	if err := proto.Unmarshal(data, p); err != nil {
		return nil, err
	}
	return p, nil
}

func splitExtentKey(k []byte) (p string, offset uint64, err error) {
	if len(k) < 9 {
		return "", 0, errors.Errorf("not extent key, too short: %q", k)
	}
	if k[len(k)-9] != 0x00 {
		return "", 0, errors.Errorf("not extent key, no NULL")
	}
	offset = binary.BigEndian.Uint64(k[len(k)-8:])
	p, err = parseMetadataKey(k[:len(k)-9])
	if err != nil {
		return "", 0, err
	}
	return p, offset, nil
}

func makeExtentKey(p string, offset uint64) []byte {
	x := makeMetadataKey(p)
	x = append(x, 0x00)
	x = appendUint64(x, offset)
	return x
}

func isExtentKey(x []byte) bool {
	return len(x) >= 9 && bytes.Index(x, []byte{0x00}) == len(x)-9
}

func fileSpanEnd(p string) []byte {
	mk := makeMetadataKey(p)
	return gotkv.PrefixEnd(mk)
}

func appendUint64(buf []byte, n uint64) []byte {
	nbytes := [8]byte{}
	binary.BigEndian.PutUint64(nbytes[:], n)
	return append(buf, nbytes[:]...)
}

func (o *Operator) getExtentF(ctx context.Context, s Store, ext *Extent, fn func(data []byte) error) error {
	ref, err := gdat.ParseRef(ext.Ref)
	if err != nil {
		return err
	}
	return o.rawOp.GetF(ctx, s, *ref, func(data []byte) error {
		if int(ext.Offset) >= len(data) {
			return errors.Errorf("extent offset %d is >= len(data) %d", ext.Offset, len(data))
		}
		data = data[ext.Offset : ext.Offset+ext.Length]
		return fn(data)
	})
}
