package gotfs

import (
	"context"
	"fmt"
	"io/fs"
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
func (mach *Machine) NewEmpty(ctx context.Context, s stores.RW) (*Root, error) {
	b := mach.NewBuilder(ctx, s, stores.NewMem())
	if err := b.Mkdir("", 0o755); err != nil {
		return nil, err
	}
	return b.Finish()
}

// Mkdir creates a directory at path p
func (mach *Machine) Mkdir(ctx context.Context, s stores.RW, x Root, p string) (*Root, error) {
	p = cleanPath(p)
	if err := mach.checkNoEntry(ctx, s, x, p); err != nil {
		return nil, err
	}
	md := &Info{
		Mode: fs.FileMode(0o755 | os.ModeDir),
	}
	return mach.PutInfo(ctx, s, x, p, md)
}

// MkdirAll creates the directory p and any of p's ancestors if necessary.
func (mach *Machine) MkdirAll(ctx context.Context, s stores.RW, x Root, p string) (*Root, error) {
	p = cleanPath(p)
	parts := strings.Split(p, string(Sep))
	y := &x
	var err error
	y, err = mach.ensureDir(ctx, s, x, "")
	if err != nil {
		return nil, err
	}
	for i := range parts {
		p2 := strings.Join(parts[:i+1], string(Sep))
		y, err = mach.ensureDir(ctx, s, *y, p2)
		if err != nil {
			return nil, err
		}
	}
	return y, nil
}

func (mach *Machine) ensureDir(ctx context.Context, s stores.RW, x Root, p string) (*Root, error) {
	y := &x
	_, err := mach.GetDirInfo(ctx, s, x, p)
	if posixfs.IsErrNotExist(err) {
		y, err = mach.Mkdir(ctx, s, x, p)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return y, nil
}

// ReadDir calls fn for every child of the directory at p.
func (mach *Machine) ReadDir(ctx context.Context, s stores.Reading, x Root, p string, fn func(e DirEnt) error) error {
	p = cleanPath(p)
	di, err := mach.newDirIterator(ctx, s, x, p)
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

func (mach *Machine) RemoveAll(ctx context.Context, s Store, x Root, p string) (*Root, error) {
	p = cleanPath(p)
	_, err := mach.GetInfo(ctx, s, x, p)
	if os.IsNotExist(err) {
		return &x, nil
	}
	if err != nil {
		return nil, err
	}
	k := newInfoKey(p)
	span := gotkv.PrefixSpan(k.Prefix(nil))
	root, err := mach.gotkv.DeleteSpan(ctx, s, *x.toGotKV(), span)
	return newRoot(root), err
}

func SpanForPath(p string) gotkv.Span {
	k := newInfoKey(p)
	return k.ChildrenSpan()
}

type dirIterator struct {
	s    stores.Reading
	x    Root
	p    string
	iter *gotkv.Iterator
}

func (mach *Machine) newDirIterator(ctx context.Context, s stores.Reading, x Root, p string) (*dirIterator, error) {
	_, err := mach.GetDirInfo(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	infoKey := newInfoKey(p)
	span := gotkv.PrefixSpan(infoKey.Prefix(nil))
	iter := mach.gotkv.NewIterator(s, *x.toGotKV(), span)
	ent := &gotkv.Entry{}
	if err := streams.NextUnit(ctx, iter, ent); err != nil {
		return nil, err
	}
	var key Key
	if err := unmarshalInfoKey(ent.Key, &key); err != nil {
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
	if err := streams.NextUnit(ctx, di.iter, &ent); err != nil {
		return nil, err
	}
	if isExtentKey(ent.Key) {
		return nil, fmt.Errorf("got extent key while iterating directory entries %q", ent.Key)
	}

	md, err := parseInfo(ent.Value)
	if err != nil {
		return nil, err
	}
	var key Key
	if err := unmarshalInfoKey(ent.Key, &key); err != nil {
		return nil, err
	}
	p := key.Path()
	name := cleanName(p[len(di.p):])
	dirEnt := DirEnt{
		Name: name,
		Mode: os.FileMode(md.Mode),
	}

	// now we have to advance through the file or directory to fully consume it.
	prefix := newInfoKey(p).Prefix(nil)
	if len(prefix) > 0 {
		prefix = prefix[:len(prefix)-1]
	}
	end := gotkv.PrefixEnd(prefix)
	if err := di.iter.Seek(ctx, end); err != nil {
		return nil, err
	}
	return &dirEnt, nil
}
