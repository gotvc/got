package gotfs

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/posixfs"
)

const Sep = '/'

type DirEnt struct {
	Name string
	Mode os.FileMode
}

// NewEmpty creates a new filesystem with an empty root directory
func (a *Machine) NewEmpty(ctx context.Context, s stores.RW) (*Root, error) {
	b := a.NewBuilder(ctx, s, stores.NewVoid())
	if err := b.Mkdir("/", 0o755); err != nil {
		return nil, err
	}
	return b.Finish()
}

// Mkdir creates a directory at path p
func (a *Machine) Mkdir(ctx context.Context, s stores.RW, x Root, p string) (*Root, error) {
	p = cleanPath(p)
	if err := a.checkNoEntry(ctx, s, x, p); err != nil {
		return nil, err
	}
	md := &Info{
		Mode: uint32(0o755 | os.ModeDir),
	}
	return a.PutInfo(ctx, s, x, p, md)
}

// MkdirAll creates the directory p and any of p's ancestors if necessary.
func (a *Machine) MkdirAll(ctx context.Context, s stores.RW, x Root, p string) (*Root, error) {
	p = cleanPath(p)
	parts := strings.Split(p, string(Sep))
	y := &x
	var err error
	y, err = a.ensureDir(ctx, s, x, "")
	if err != nil {
		return nil, err
	}
	for i := range parts {
		p2 := strings.Join(parts[:i+1], string(Sep))
		y, err = a.ensureDir(ctx, s, *y, p2)
		if err != nil {
			return nil, err
		}
	}
	return y, nil
}

func (a *Machine) ensureDir(ctx context.Context, s stores.RW, x Root, p string) (*Root, error) {
	y := &x
	_, err := a.GetDirInfo(ctx, s, x, p)
	if posixfs.IsErrNotExist(err) {
		y, err = a.Mkdir(ctx, s, x, p)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return y, nil
}

// ReadDir calls fn for every child of the directory at p.
func (a *Machine) ReadDir(ctx context.Context, s stores.Reading, x Root, p string, fn func(e DirEnt) error) error {
	p = cleanPath(p)
	di, err := a.newDirIterator(ctx, s, x, p)
	if err != nil {
		return err
	}
	for {
		dirEnt, err := di.Next(ctx)
		if streams.IsEOS(err) {
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

func (a *Machine) RemoveAll(ctx context.Context, s Store, x Root, p string) (*Root, error) {
	p = cleanPath(p)
	_, err := a.GetInfo(ctx, s, x, p)
	if os.IsNotExist(err) {
		return &x, nil
	}
	if err != nil {
		return nil, err
	}
	k := makeInfoKey(p)
	span := gotkv.PrefixSpan(k)
	root, err := a.gotkv.DeleteSpan(ctx, s, *x.toGotKV(), span)
	return newRoot(root), err
}

func dirSpan(p string) gotkv.Span {
	p = cleanPath(p)
	k := makeInfoKey(p)
	return gotkv.PrefixSpan(k)
}

func SpanForPath(p string) gotkv.Span {
	p = cleanPath(p)
	k := makeInfoKey(p)
	return gotkv.PrefixSpan(k)
}

type dirIterator struct {
	s    stores.Reading
	x    Root
	p    string
	iter *gotkv.Iterator
}

func (a *Machine) newDirIterator(ctx context.Context, s stores.Reading, x Root, p string) (*dirIterator, error) {
	_, err := a.GetDirInfo(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	span := dirSpan(p)
	iter := a.gotkv.NewIterator(s, *x.toGotKV(), span)
	ent := &gotkv.Entry{}
	if err := iter.Next(ctx, ent); err != nil {
		return nil, err
	}
	if _, err = parseInfoKey(ent.Key); err != nil {
		return nil, err
	}
	if _, err := parseInfo(ent.Value); err != nil {
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
	var ent gotkv.Entry
	if err := di.iter.Next(ctx, &ent); err != nil {
		return nil, err
	}
	if isExtentKey(ent.Key) {
		return nil, fmt.Errorf("got extent key while iterating directory entries %q", ent.Key)
	}
	md, err := parseInfo(ent.Value)
	if err != nil {
		return nil, err
	}
	// now we have to advance through the file or directory to fully consume it.
	end := gotkv.PrefixEnd(ent.Key)
	if err := di.iter.Seek(ctx, end); err != nil {
		return nil, err
	}
	p, err := parseInfoKey(ent.Key)
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
