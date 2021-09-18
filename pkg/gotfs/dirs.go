package gotfs

import (
	"context"
	"io"
	"os"
	"path"
	"strings"

	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
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
	p = cleanPath(p)
	if err := o.checkNoEntry(ctx, s, x, p); err != nil {
		return nil, err
	}
	md := &Metadata{
		Mode: uint32(0o755 | os.ModeDir),
	}
	return o.PutMetadata(ctx, s, x, p, md)
}

// MkdirAll creates the directory p and any of p's ancestors if necessary.
func (o *Operator) MkdirAll(ctx context.Context, s Store, x Root, p string) (*Root, error) {
	p = cleanPath(p)
	parts := strings.Split(p, string(Sep))
	y := &x
	var err error
	y, err = o.ensureDir(ctx, s, x, "")
	if err != nil {
		return nil, err
	}
	for i := range parts {
		p2 := strings.Join(parts[:i+1], string(Sep))
		y, err = o.ensureDir(ctx, s, *y, p2)
		if err != nil {
			return nil, err
		}
	}
	return y, nil
}

func (o *Operator) ensureDir(ctx context.Context, s Store, x Root, p string) (*Root, error) {
	y := &x
	_, err := o.GetDirMetadata(ctx, s, x, p)
	if posixfs.IsErrNotExist(err) {
		y, err = o.Mkdir(ctx, s, x, p)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return y, nil
}

// ReadDir calls fn for every child of the directory at p.
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
	p = cleanPath(p)
	_, err := o.GetMetadata(ctx, s, x, p)
	if os.IsNotExist(err) {
		return &x, nil
	}
	if err != nil {
		return nil, err
	}
	k := makeMetadataKey(p)
	span := gotkv.PrefixSpan(k)
	return o.gotkv.DeleteSpan(ctx, s, x, span)
}

func cleanPath(p string) string {
	p = path.Clean(p)
	if p == "." {
		return ""
	}
	return strings.Trim(p, string(Sep))
}

func cleanName(p string) string {
	return strings.Trim(p, string(Sep))
}

func dirSpan(p string) gotkv.Span {
	p = cleanPath(p)
	k := makeMetadataKey(p)
	return gotkv.PrefixSpan(k)
}

func SpanForPath(p string) gotkv.Span {
	p = cleanPath(p)
	k := makeMetadataKey(p)
	return gotkv.PrefixSpan(k)
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
	span := dirSpan(p)
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
	if isExtentKey(ent.Key) {
		return nil, errors.Errorf("got part key while iterating directory entries")
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
	p, err := parseMetadataKey(ent.Key)
	if err != nil {
		return nil, err
	}
	name := cleanName(p[len(di.p):])
	dirEnt := DirEnt{
		Name: name,
		Mode: os.FileMode(md.Mode),
	}
	return &dirEnt, nil
}
