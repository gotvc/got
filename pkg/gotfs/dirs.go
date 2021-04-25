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

// NewEmpty creates a new filesystem with nothing in it.
func (o *Operator) NewEmpty(ctx context.Context, s Store) (*Root, error) {
	return o.gotkv.NewEmpty(ctx, s)
}

// Mkdir creates a directory at path p
func (o *Operator) Mkdir(ctx context.Context, s Store, x Root, p string) (*Root, error) {
	if err := o.checkNoEntry(ctx, s, x, p); err != nil {
		return nil, err
	}
	md := &Metadata{
		Mode: uint32(0o755 | os.ModeDir),
	}
	return o.PutMetadata(ctx, s, x, p, md)
}

func (o *Operator) EnsureDir(ctx context.Context, s Store, x Root, p string) (*Root, error) {
	p = cleanPath(p)
	parts := strings.Split(p, string(Sep))
	for i := range parts {
		p2 := strings.Join(parts[:i+1], string(Sep))
		_, err := o.GetDirMetadata(ctx, s, x, p2)
		if os.IsNotExist(err) {
			x2, err := o.Mkdir(ctx, s, x, p2)
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

func (o *Operator) ReadDir(ctx context.Context, s Store, x Root, p string, fn func(e DirEnt) error) error {
	p = cleanPath(p)
	di, err := o.newDirIterator(ctx, s, x, p)
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

func (o *Operator) RemoveAll(ctx context.Context, s Store, x Root, p string) (*Root, error) {
	span := gotkv.PrefixSpan([]byte(p))
	return o.gotkv.DeleteSpan(ctx, s, x, span)
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

func (o *Operator) newDirIterator(ctx context.Context, s Store, x Root, p string) (*dirIterator, error) {
	_, err := o.GetDirMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	span := gotkv.PrefixSpan([]byte(p))
	iter := o.gotkv.NewIterator(s, x, span)
	if _, err := iter.Next(ctx); err != nil {
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
	end := gotkv.PrefixEnd(ent.Key)
	if err := di.iter.Seek(ctx, end); err != nil {
		return nil, err
	}
	name := cleanPath(string(ent.Key[len(di.p):]))
	dirEnt := DirEnt{
		Name: name,
		Mode: os.FileMode(md.Mode),
	}
	return &dirEnt, nil
}
