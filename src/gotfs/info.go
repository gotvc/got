package gotfs

import (
	"context"
	"fmt"
	"os"
	"strings"

	"errors"

	"capnproto.org/go/capnp/v3"
	"github.com/gotvc/got/src/gotfs/gotfscnp"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
)

type Info struct {
	Mode  uint32
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
	infoc.SetMode(info.Mode)
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
	info.Mode = infoc.Mode()
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

func makeInfoKey(p string) []byte {
	p = cleanPath(p)
	if p == "" {
		return []byte("/")
	}
	return []byte("/" + p + "/")
}

func parseInfoKey(k []byte) (string, error) {
	switch len(k) {
	case 0:
		return "", fmt.Errorf("not a valid metadata key: %q", k)
	case 1:
		p := string(k)
		if p[0] == Sep {
			return p, nil
		}
		return "", fmt.Errorf("not a valid metadata key: %q", k)
	default:
		if k[0] != Sep || k[len(k)-1] != Sep {
			return "", fmt.Errorf("not a valid metadata key: %q", k)
		}
		return string(k[1 : len(k)-1]), nil
	}
}

// PutInfo assigns metadata to p
func (a *Machine) PutInfo(ctx context.Context, s stores.RW, x Root, p string, md *Info) (*Root, error) {
	p = cleanPath(p)
	if err := checkPath(p); err != nil {
		return nil, err
	}
	k := makeInfoKey(p)
	root, err := a.gotkv.Put(ctx, s, *x.toGotKV(), k, md.marshal())
	return newRoot(root), err
}

// GetInfo retrieves the metadata at p if it exists and errors otherwise
func (a *Machine) GetInfo(ctx context.Context, s stores.Reading, x Root, p string) (*Info, error) {
	return a.getInfo(ctx, s, x.ToGotKV(), p)
}

func (a *Machine) getInfo(ctx context.Context, s stores.Reading, x gotkv.Root, p string) (*Info, error) {
	p = cleanPath(p)
	var md *Info
	err := a.gotkv.GetF(ctx, s, x, makeInfoKey(p), func(data []byte) error {
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
func (a *Machine) GetDirInfo(ctx context.Context, s stores.Reading, x Root, p string) (*Info, error) {
	md, err := a.GetInfo(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !os.FileMode(md.Mode).IsDir() {
		return nil, fmt.Errorf("%s is not a directory", p)
	}
	return md, nil
}

// GetFileInfo returns the file metadata at p if it exists, and errors otherwise
func (a *Machine) GetFileInfo(ctx context.Context, s stores.Reading, x Root, p string) (*Info, error) {
	md, err := a.GetInfo(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !os.FileMode(md.Mode).IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", p)
	}
	return md, nil
}

func (a *Machine) checkNoEntry(ctx context.Context, s stores.Reading, x Root, p string) error {
	_, err := a.GetInfo(ctx, s, x, p)
	switch {
	case err == os.ErrNotExist:
		return nil
	case err == nil:
		return os.ErrExist
	default:
		return err
	}
}

func checkPath(p string) error {
	if len(p) > MaxPathLen {
		return fmt.Errorf("path too long: %q", p)
	}
	if strings.ContainsAny(p, "\x00") {
		return fmt.Errorf("path cannot contain null")
	}
	return nil
}

func parentPath(x string) string {
	x = cleanPath(x)
	parts := strings.Split(x, string(Sep))
	if len(parts) == 0 {
		panic("no parent of empty path")
	}
	if len(parts) == 1 {
		return ""
	}
	return strings.Join(parts[:len(parts)-1], string(Sep))
}
