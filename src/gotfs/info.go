package gotfs

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	"errors"

	"capnproto.org/go/capnp/v3"
	"github.com/gotvc/got/src/gotfs/gotfscnp"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

type Info struct {
	Mode  fs.FileMode
	Attrs map[string][]byte
}

func (info *Info) marshal() []byte {
	if info == nil {
		panic("info is nil")
	}
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		panic(err)
	}
	infoc, err := gotfscnp.NewRootInfo(seg)
	if err != nil {
		panic(err)
	}
	infoc.SetMode(uint32(info.Mode))
	al, err := infoc.NewAttrs(int32(len(info.Attrs)))
	if err != nil {
		panic(err)
	}
	var acount int
	for k, v := range info.Attrs {
		attr := al.At(acount)
		attr.SetKey(k)
		attr.SetValue(v)
		acount++
	}
	data, err := capnp.Canonicalize(capnp.Struct(infoc))
	if err != nil {
		panic(err)
	}
	return data
}

func (info *Info) unmarshal(data []byte) error {
	msg, _, err := capnp.NewMessage(capnp.SingleSegment(data))
	if err != nil {
		return err
	}
	infoc, err := gotfscnp.ReadRootInfo(msg)
	if err != nil {
		return err
	}
	al, err := infoc.Attrs()
	if err != nil {
		return err
	}
	attrs := make(map[string][]byte)
	for i := 0; i < al.Len(); i++ {
		attr := al.At(i)
		key, err := attr.Key()
		if err != nil {
			return err
		}
		value, err := attr.Value()
		if err != nil {
			return err
		}
		attrs[key] = value
	}
	info.Mode = fs.FileMode(infoc.Mode())
	info.Attrs = attrs
	return nil
}

func parseInfo(data []byte) (*Info, error) {
	var ret Info
	if err := ret.unmarshal(data); err != nil {
		return nil, err
	}
	return &ret, nil
}

// PutInfo assigns metadata to p
func (mach *Machine) PutInfo(ctx context.Context, s stores.RW, x Root, p string, md *Info) (*Root, error) {
	p = cleanPath(p)
	if err := checkPath(p); err != nil {
		return nil, err
	}
	k := newInfoKey(p)
	root, err := mach.gotkv.Put(ctx, s, *x.toGotKV(), k.Marshal(nil), md.marshal())
	return newRoot(root), err
}

// GetInfo retrieves the metadata at p if it exists and errors otherwise
func (mach *Machine) GetInfo(ctx context.Context, s stores.Reading, x Root, p string) (*Info, error) {
	return mach.getInfo(ctx, s, x.ToGotKV(), p)
}

func (mach *Machine) getInfo(ctx context.Context, s stores.Reading, x gotkv.Root, p string) (*Info, error) {
	p = cleanPath(p)
	var md *Info
	err := mach.gotkv.GetF(ctx, s, x, newInfoKey(p).Marshal(nil), func(data []byte) error {
		var err error
		md, err = parseInfo(data)
		return err
	})
	if err != nil {
		if errors.Is(err, gotkv.ErrKeyNotFound) {
			err = os.ErrNotExist
		}
		return nil, err
	}
	return md, nil
}

// GetDirInfo returns directory metadata at p if it exists, and errors otherwise
func (mach *Machine) GetDirInfo(ctx context.Context, s stores.Reading, x Root, p string) (*Info, error) {
	md, err := mach.GetInfo(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !os.FileMode(md.Mode).IsDir() {
		return nil, fmt.Errorf("%s is not a directory", p)
	}
	return md, nil
}

// GetFileInfo returns the file metadata at p if it exists, and errors otherwise
func (mach *Machine) GetFileInfo(ctx context.Context, s stores.Reading, x Root, p string) (*Info, error) {
	md, err := mach.GetInfo(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !os.FileMode(md.Mode).IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", p)
	}
	return md, nil
}

func (mach *Machine) checkNoEntry(ctx context.Context, s stores.Reading, x Root, p string) error {
	_, err := mach.GetInfo(ctx, s, x, p)
	switch {
	case err == os.ErrNotExist:
		return nil
	case err == nil:
		return os.ErrExist
	default:
		return err
	}
}

// InfoEntry is the Path and the info it points to.
type InfoEntry struct {
	Path string
	Info Info
}

var _ streams.Iterator[InfoEntry] = &InfoIterator{}

type InfoIterator struct {
	m    *Machine
	s    stores.Reading
	root Root
}

func (m *Machine) NewInfoIterator(ms stores.Reading, root Root, p string) *InfoIterator {
	return &InfoIterator{m: m, s: ms, root: root}
}

func (m *InfoIterator) Next(ctx context.Context, dst []InfoEntry) (int, error) {
	return 0, streams.EOS()
}
