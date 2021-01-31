package gotfs

import (
	"context"
	"io"
	"math"
	"os"
	"strings"

	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

const Sep = '/'

func GetDirMetadata(ctx context.Context, s Store, x Ref, p string) (*Metadata, error) {
	md, err := GetMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !md.Mode.IsDir() {
		return nil, errors.Errorf("%s is not a directory", p)
	}
	return md, nil
}

type DirEnt struct {
	Name string
	Mode os.FileMode
}

func Mkdir(ctx context.Context, s Store, x Ref, p string) (*Ref, error) {
	if err := checkNoEntry(ctx, s, x, p); err != nil {
		return nil, err
	}
	md := Metadata{
		Mode: 0o755 | os.ModeDir,
	}
	return PutMetadata(ctx, s, x, p, md)
}

func EnsureDir(ctx context.Context, s Store, x Ref, p string) (*Ref, error) {
	parts := strings.Split(p, string(Sep))
	for i := range parts {
		p2 := strings.Join(parts[:i+1], string(Sep))
		_, err := GetDirMetadata(ctx, s, x, p2)
		if os.IsNotExist(err) {
			x2, err := Mkdir(ctx, s, x, parts[0])
			if err != nil {
				return nil, err
			}
			x = *x2
		} else if err != nil {
			return nil, err
		}
	}
	return &x, nil
}

func ReadDir(ctx context.Context, s Store, x Ref, p string, fn func(e DirEnt) error) error {
	p = cleanPath(p)
	di, err := newDirIterator(ctx, s, x, p)
	if err != nil {
		return err
	}
	for {
		err := di.Next(ctx, fn)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func RemoveAll(ctx context.Context, s Store, x Ref, p string) (*Ref, error) {
	return gotkv.DeletePrefix(ctx, s, x, []byte(p))
}

func PrependDirs(ctx context.Context, s Store, x Ref, prefix string) (*Ref, error) {
	// TODO: add dir metadata
	return gotkv.AddPrefix(ctx, s, x, []byte(prefix))
}

func Merge(ctx context.Context, s Store, xs []Ref) (*Ref, error) {
	return gotkv.Reduce(ctx, s, xs, func(key []byte, v1, v2 []byte) ([]byte, error) {
		panic("")
	})
}

func cleanPath(p string) string {
	p = strings.Trim(p, string(Sep))
	return p
}

type dirIterator struct {
	s    Store
	x    Ref
	p    string
	iter *gotkv.Iterator
}

func newDirIterator(ctx context.Context, s Store, x Ref, p string) (*dirIterator, error) {
	_, err := GetDirMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	iter := gotkv.NewIterator(ctx, s, x)
	iter.SeekPast([]byte(p))
	return &dirIterator{
		s:    s,
		x:    x,
		p:    p,
		iter: iter,
	}, nil
}

func (di *dirIterator) Next(ctx context.Context, fn func(de DirEnt) error) error {
	var dirEnt DirEnt
	var seekPast []byte
	if err := di.iter.Next(func(key, value []byte) error {
		md, err := parseMetadata(value)
		if err != nil {
			return err
		}
		dirEnt = DirEnt{
			Name: string(key[len(di.p):]),
			Mode: md.Mode,
		}
		seekPast = append([]byte{}, key...)
		seekPast = appendUint64(seekPast, math.MaxUint64)
		return nil
	}); err != nil {
		return err
	}
	if err := fn(dirEnt); err != nil {
		return err
	}
	di.iter.SeekPast(seekPast)
	return nil
}
