package gotfs

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"errors"

	"capnproto.org/go/capnp/v3"
	"github.com/gotvc/got/src/gotfs/gotfscnp"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
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

// markInfoKey creates a key to store the Info object for a path p
// It consists of the path prefix, and the
func makeInfoKey(p string) (out []byte) {
	out = appendPrefix(out, p)
	out = append(out, 0)                        // NULL
	out = binary.BigEndian.AppendUint64(out, 0) // 0 offset
	return out
}

func parseInfoKey(k []byte) (string, error) {
	if !isInfoKey(k) {
		return "", fmt.Errorf("not a valid metdata key: %q", k)
	}
	// at this point we know the key is >= 9 bytes long.
	p := string(k[:len(k)-9])
	return cleanPath(p), nil
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
