package gotfs

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

// CreateFileFrom creates a file at p with data from r
func (o *Operator) CreateFileFrom(ctx context.Context, s Store, x Root, p string, r io.Reader) (*gotkv.Root, error) {
	if err := o.checkNoEntry(ctx, s, x, p); err != nil {
		return nil, err
	}
	// TODO: add this back after we have migrated to the builder.
	// as := cadata.NewAsyncStore(s, runtime.GOMAXPROCS(0))
	// s = as
	kvop := gotkv.NewOperator()
	dop := gdat.NewOperator()
	// create metadata entry
	md := Metadata{
		Mode: 0o644,
	}
	if x2, err := o.PutMetadata(ctx, s, x, p, md); err != nil {
		return nil, err
	} else {
		x = *x2
	}
	// add file data
	var total uint64
	buf := make([]byte, maxPartSize)
	for done := false; !done; {
		n, err := r.Read(buf)
		if err == io.EOF {
			done = true
		} else if err != nil {
			return nil, err
		}
		if n < 1 {
			continue
		}
		ref, err := dop.Post(ctx, s, buf[:n])
		if err != nil {
			return nil, err
		}
		part := Part{
			Ref:    *ref,
			Offset: 0,
			Length: uint32(n),
		}
		key := makePartKey(p, total)

		if x2, err := kvop.Put(ctx, s, x, key, part.marshal()); err != nil {
			return nil, err
		} else {
			x = *x2
		}
		total += uint64(n)
	}
	return &x, nil
}

func (o *Operator) SizeOfFile(ctx context.Context, s Store, x Root, p string) (int, error) {
	gotkv := gotkv.NewOperator()
	key, err := gotkv.MaxKey(ctx, s, x, []byte(p))
	if err != nil {
		return 0, err
	}
	// offset of key
	if len(key) < 8 {
		return 0, errors.Errorf("key too short")
	}
	offset := binary.BigEndian.Uint64(key[len(key)-8:])
	// size of part at that key
	var size int
	if err := gotkv.GetF(ctx, s, x, []byte(p), func(v []byte) error {
		size = len(v)
		return nil
	}); err != nil {
		return 0, err
	}
	return int(offset) + size, nil
}

func (o *Operator) ReadFileAt(ctx context.Context, s Store, x Root, p string, start uint64, buf []byte) (int, error) {
	kvo := gotkv.NewOperator()
	do := gdat.NewOperator()
	_, err := o.GetFileMetadata(ctx, s, x, p)
	if err != nil {
		return 0, err
	}
	offset := start - (start % maxPartSize)
	key := makePartKey(p, offset)
	var n int
	err = kvo.GetF(ctx, s, x, key, func(data []byte) error {
		part, err := parsePart(data)
		if err != nil {
			return err
		}
		return do.GetF(ctx, s, part.Ref, func(data []byte) error {
			begin := int(part.Offset)
			if begin >= len(data) {
				return errors.Errorf("incorrect offset")
			}
			end := int(part.Offset + part.Length)
			if end > len(data) {
				return errors.Errorf("incorrect length")
			}
			n = copy(buf, data[begin:end])
			return nil
		})
	})
	if err == gotkv.ErrKeyNotFound {
		err = io.EOF
	}
	return n, err
}

func (o *Operator) WriteFileAt(ctx context.Context, s Store, x Root, p string, start uint64, data []byte) (*Ref, error) {
	md, err := o.GetFileMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	panic(md)
}

func appendUint64(buf []byte, n uint64) []byte {
	nbytes := [8]byte{}
	binary.BigEndian.PutUint64(nbytes[:], n)
	return append(buf, nbytes[:]...)
}
