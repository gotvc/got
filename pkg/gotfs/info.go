package gotfs

import (
	"context"
	"fmt"
	"os"
	"strings"

	"errors"

	"github.com/gotvc/got/pkg/gotkv"
	"google.golang.org/protobuf/proto"
)

func (m *Info) marshal() []byte {
	data, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return data
}

func parseInfo(data []byte) (*Info, error) {
	md := &Info{}
	if err := proto.Unmarshal(data, md); err != nil {
		return nil, err
	}
	return md, nil
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
func (a *Machine) PutInfo(ctx context.Context, s Store, x Root, p string, md *Info) (*Root, error) {
	p = cleanPath(p)
	if err := checkPath(p); err != nil {
		return nil, err
	}
	k := makeInfoKey(p)
	root, err := a.gotkv.Put(ctx, s, *x.toGotKV(), k, md.marshal())
	return newRoot(root), err
}

// GetInfo retrieves the metadata at p if it exists and errors otherwise
func (a *Machine) GetInfo(ctx context.Context, s Store, x Root, p string) (*Info, error) {
	return a.getInfo(ctx, s, x.ToGotKV(), p)
}

func (a *Machine) getInfo(ctx context.Context, s Store, x gotkv.Root, p string) (*Info, error) {
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
func (a *Machine) GetDirInfo(ctx context.Context, s Store, x Root, p string) (*Info, error) {
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
func (a *Machine) GetFileInfo(ctx context.Context, s Store, x Root, p string) (*Info, error) {
	md, err := a.GetInfo(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !os.FileMode(md.Mode).IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", p)
	}
	return md, nil
}

func (a *Machine) checkNoEntry(ctx context.Context, s Store, x Root, p string) error {
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
