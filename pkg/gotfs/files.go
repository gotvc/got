package gotfs

import (
	"context"
	"encoding/binary"
	"io"
	"runtime"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/brendoncarroll/got/pkg/refs"
	"github.com/pkg/errors"
)

const maxPartSize = 1 << 15

type (
	Ref   = gotkv.Ref
	Store = gotkv.Store
	Root  = gotkv.Root
)

func New(ctx context.Context, s Store) (*Root, error) {
	op := gotkv.NewOperator()
	x, err := op.NewEmpty(ctx, s)
	if err != nil {
		return nil, err
	}
	return Mkdir(ctx, s, *x, "")
}

func CreateFileFrom(ctx context.Context, s Store, x Root, p string, r io.Reader) (*gotkv.Root, error) {
	if err := checkNoEntry(ctx, s, x, p); err != nil {
		return nil, err
	}
	op := gotkv.NewOperator()
	as := cadata.NewAsyncStore(s, runtime.GOMAXPROCS(0))
	// create metadata entry
	md := Metadata{
		Mode: 0o644,
	}
	if x2, err := PutMetadata(ctx, s, x, p, md); err != nil {
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
		ref, err := refs.Post(ctx, as, buf[:n])
		if err != nil {
			return nil, err
		}
		part := Part{
			Ref:    *ref,
			Offset: 0,
			Length: uint32(n),
		}
		key := makePartKey(p, total)

		if x2, err := op.Put(ctx, s, x, key, part.marshal()); err != nil {
			return nil, err
		} else {
			x = *x2
		}
		total += uint64(n)
	}
	return &x, as.Close()
}

func SizeOfFile(ctx context.Context, s Store, x Root, p string) (int, error) {
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

func ReadFileAt(ctx context.Context, s Store, x Root, p string, start uint64, buf []byte) (int, error) {
	op := gotkv.NewOperator()
	_, err := GetFileMetadata(ctx, s, x, p)
	if err != nil {
		return 0, err
	}
	offset := start - (start % maxPartSize)
	key := makePartKey(p, offset)
	var n int
	err = op.GetF(ctx, s, x, key, func(data []byte) error {
		part, err := parsePart(data)
		if err != nil {
			return err
		}
		return refs.GetF(ctx, s, part.Ref, func(data []byte) error {
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

func WriteFileAt(ctx context.Context, s Store, x Root, p string, start uint64, data []byte) (*Ref, error) {
	md, err := GetFileMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	panic(md)
}

func GetFileMetadata(ctx context.Context, s Store, x Root, p string) (*Metadata, error) {
	md, err := GetMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !md.Mode.IsRegular() {
		return nil, errors.Errorf("%s is not a regular file", p)
	}
	return md, nil
}

func appendUint64(buf []byte, n uint64) []byte {
	nbytes := [8]byte{}
	binary.BigEndian.PutUint64(nbytes[:], n)
	return append(buf, nbytes[:]...)
}

func splitKey(k []byte) (p string, offset uint64, err error) {
	if len(k) < 8 {
		return "", 0, errors.Errorf("key too short")
	}
	p = string(k[:len(k)-8])
	offset = binary.BigEndian.Uint64(k[len(k)-8:])
	return p, offset, nil
}

func makePartKey(p string, offset uint64) []byte {
	x := []byte(p)
	x = appendUint64(x, offset)
	return x
}
