package gotfs

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/brendoncarroll/got/pkg/gotkv"
)

const Sep = '/'

type DirEnt struct {
	Name string
	Mode os.FileMode
}

func Mkdir(ctx context.Context, s Store, x Root, p string) (*Root, error) {
	if err := checkNoEntry(ctx, s, x, p); err != nil {
		return nil, err
	}
	md := Metadata{
		Mode: 0o755 | os.ModeDir,
	}
	return PutMetadata(ctx, s, x, p, md)
}

func EnsureDir(ctx context.Context, s Store, x Root, p string) (*Root, error) {
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

func ReadDir(ctx context.Context, s Store, x Root, p string, fn func(e DirEnt) error) error {
	p = cleanPath(p)
	di, err := newDirIterator(ctx, s, x, p)
	if err != nil {
		return err
	}
	for {
		dirEnt, err := di.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := fn(*dirEnt); err != nil {
			return err
		}
	}
	return nil
}

func RemoveAll(ctx context.Context, s Store, x Root, p string) (*Root, error) {
	op := gotkv.NewOperator()
	span := gotkv.PrefixSpan([]byte(p))
	return op.DeleteSpan(ctx, s, x, span)
}

func cleanPath(p string) string {
	p = strings.Trim(p, string(Sep))
	return p
}

type dirIterator struct {
	s    Store
	x    Root
	p    string
	iter gotkv.Iterator
}

func newDirIterator(ctx context.Context, s Store, x Root, p string) (*dirIterator, error) {
	_, err := GetDirMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	span := gotkv.PrefixSpan([]byte(p))
	iter := gotkv.NewOperator().NewIterator(s, x, span)
	if _, err := iter.Next(ctx); err != nil && err != io.EOF {
		return nil, err
	}
	return &dirIterator{
		s:    s,
		x:    x,
		p:    p,
		iter: iter,
	}, nil
}

func (di *dirIterator) Next(ctx context.Context) (*DirEnt, error) {
	ent, err := di.iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	md, err := parseMetadata(ent.Value)
	if err != nil {
		return nil, err
	}
	// now we have to advance through the file or directory to fully consume it.
	if err := di.iter.Seek(ctx, gotkv.PrefixEnd(ent.Key)); err != nil {
		return nil, err
	}
	name := string(ent.Key[len(di.p)+1:])
	dirEnt := DirEnt{
		Name: name,
		Mode: md.Mode,
	}
	return &dirEnt, nil
}
