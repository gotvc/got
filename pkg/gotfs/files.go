package gotfs

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

const maxPartSize = 1 << 15

type Ref = gotkv.Ref
type Store = gotkv.Store

func New(ctx context.Context, s Store) (*Ref, error) {
	x, err := gotkv.New(ctx, s)
	if err != nil {
		return nil, err
	}
	return Mkdir(ctx, s, *x, "")
}

func CreateFileFrom(ctx context.Context, s Store, x Ref, p string, r io.Reader) (*Ref, error) {
	if err := checkNoEntry(ctx, s, x, p); err != nil {
		return nil, err
	}
	x2, err := PutMetadata(ctx, s, x, p, Metadata{
		Mode: 0o644,
	})
	if err != nil {
		return nil, err
	}
	var total uint64
	buf := make([]byte, 4096)
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
		ref, err := gotkv.PostRaw(ctx, s, buf[:n])
		if err != nil {
			return nil, err
		}
		part := Part{
			Ref:    *ref,
			Offset: 0,
			Length: uint32(n),
		}
		key := makePartKey(p, total)
		x2, err = gotkv.Put(ctx, s, *x2, key, part.marshal())
		if err != nil {
			return nil, err
		}
		total += uint64(n)
	}
	return x2, nil
}

func SizeOfFile(ctx context.Context, s Store, x Ref, p string) (int, error) {
	key, err := gotkv.MaxKey(ctx, s, x, []byte(p))
	if err != nil {
		return 0, err
	}
	var size int
	if err := gotkv.GetF(ctx, s, x, []byte(p), func(v []byte) error {
		size = len(v)
		return nil
	}); err != nil {
		return 0, err
	}
	if len(key) < 8 {
		return 0, errors.Errorf("key too short")
	}
	offset := binary.BigEndian.Uint64(key[len(key)-8:])
	return int(offset) + size, nil
}

func ReadFileAt(ctx context.Context, s Store, x Ref, p string, start uint64, buf []byte) (int, error) {
	_, err := GetFileMetadata(ctx, s, x, p)
	if err != nil {
		return 0, err
	}
	offset := start - (start % maxPartSize)
	key := makePartKey(p, offset)
	var n int
	err = gotkv.GetF(ctx, s, x, key, func(data []byte) error {
		part, err := parsePart(data)
		if err != nil {
			return err
		}
		return gotkv.GetRawF(ctx, s, part.Ref, func(data []byte) error {
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

func WriteFileAt(ctx context.Context, s Store, x Ref, p string, start uint64, data []byte) (*Ref, error) {
	md, err := GetFileMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	panic(md)
}

func GetFileMetadata(ctx context.Context, s Store, x Ref, p string) (*Metadata, error) {
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
