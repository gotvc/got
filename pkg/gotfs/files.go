package gotfs

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

type Ref = gotkv.Ref
type Store = gotkv.Store

type Part struct {
	Ref    Ref
	Offset uint32
	Length uint32
}

func (p *Part) Marshal() []byte {
	data, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return data
}

func New(ctx context.Context, s Store) (*Ref, error) {
	return gotkv.New(ctx, s)
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
		key := []byte(p)
		key = appendUint64(key, total)
		x2, err = gotkv.Put(ctx, s, *x2, key, part.Marshal())
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

func ReadFileAt(ctx context.Context, s Store, x Ref, p string, start uint64, data []byte) (int, error) {
	md, err := GetFileMetadata(ctx, s, x, p)
	if err != nil {
		return 0, err
	}
	panic(md)
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
