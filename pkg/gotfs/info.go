package gotfs

import (
	"context"
	"os"
	"strings"

	"github.com/gotvc/got/pkg/gotkv"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const MaxPathLen = 4096

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
		return "", errors.Errorf("not a valid metadata key: %q", k)
	case 1:
		p := string(k)
		if p[0] == Sep {
			return p, nil
		}
		return "", errors.Errorf("not a valid metadata key: %q", k)
	default:
		if k[0] != Sep || k[len(k)-1] != Sep {
			return "", errors.Errorf("not a valid metadata key: %q", k)
		}
		return string(k[1 : len(k)-1]), nil
	}
}

// PutInfo assigns metadata to p
func (o *Operator) PutInfo(ctx context.Context, s Store, x Root, p string, md *Info) (*Root, error) {
	p = cleanPath(p)
	if err := checkPath(p); err != nil {
		return nil, err
	}
	k := makeInfoKey(p)
	return o.gotkv.Put(ctx, s, x, k, md.marshal())
}

// GetInfo retrieves the metadata at p if it exists and errors otherwise
func (o *Operator) GetInfo(ctx context.Context, s Store, x Root, p string) (*Info, error) {
	p = cleanPath(p)
	var md *Info
	err := o.gotkv.GetF(ctx, s, x, makeInfoKey(p), func(data []byte) error {
		var err error
		md, err = parseInfo(data)
		return err
	})
	if err != nil {
		if err == gotkv.ErrKeyNotFound {
			err = os.ErrNotExist
		}
		return nil, err
	}
	return md, nil
}

// GetDirInfo returns directory metadata at p if it exists, and errors otherwise
func (o *Operator) GetDirInfo(ctx context.Context, s Store, x Root, p string) (*Info, error) {
	md, err := o.GetInfo(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !os.FileMode(md.Mode).IsDir() {
		return nil, errors.Errorf("%s is not a directory", p)
	}
	return md, nil
}

// GetFileInfo returns the file metadata at p if it exists, and errors otherwise
func (o *Operator) GetFileInfo(ctx context.Context, s Store, x Root, p string) (*Info, error) {
	md, err := o.GetInfo(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !os.FileMode(md.Mode).IsRegular() {
		return nil, errors.Errorf("%s is not a regular file", p)
	}
	return md, nil
}

func (o *Operator) checkNoEntry(ctx context.Context, s Store, x Root, p string) error {
	_, err := o.GetInfo(ctx, s, x, p)
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
		return errors.Errorf("path too long: %q", p)
	}
	if strings.ContainsAny(p, "\x00") {
		return errors.Errorf("path cannot contain null")
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
