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

func (m *Metadata) marshal() []byte {
	data, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return data
}

func parseMetadata(data []byte) (*Metadata, error) {
	md := &Metadata{}
	if err := proto.Unmarshal(data, md); err != nil {
		return nil, err
	}
	return md, nil
}

func makeMetadataKey(p string) []byte {
	p = cleanPath(p)
	if p == "" {
		return []byte("/")
	}
	return []byte("/" + p + "/")
}

func parseMetadataKey(k []byte) (string, error) {
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

// PutMetadata assigns metadata to p
func (o *Operator) PutMetadata(ctx context.Context, s Store, x Root, p string, md *Metadata) (*Root, error) {
	p = cleanPath(p)
	if err := checkPath(p); err != nil {
		return nil, err
	}
	k := makeMetadataKey(p)
	return o.gotkv.Put(ctx, s, x, k, md.marshal())
}

// GetMetadata retrieves the metadata at p if it exists and errors otherwise
func (o *Operator) GetMetadata(ctx context.Context, s Store, x Root, p string) (*Metadata, error) {
	p = cleanPath(p)
	var md *Metadata
	err := o.gotkv.GetF(ctx, s, x, makeMetadataKey(p), func(data []byte) error {
		var err error
		md, err = parseMetadata(data)
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

// GetDirMetadata returns directory metadata at p if it exists, and errors otherwise
func (o *Operator) GetDirMetadata(ctx context.Context, s Store, x Root, p string) (*Metadata, error) {
	md, err := o.GetMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !os.FileMode(md.Mode).IsDir() {
		return nil, errors.Errorf("%s is not a directory", p)
	}
	return md, nil
}

// GetFileMetadata returns the file metadata at p if it exists, and errors otherwise
func (o *Operator) GetFileMetadata(ctx context.Context, s Store, x Root, p string) (*Metadata, error) {
	md, err := o.GetMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !os.FileMode(md.Mode).IsRegular() {
		return nil, errors.Errorf("%s is not a regular file", p)
	}
	return md, nil
}

func (o *Operator) checkNoEntry(ctx context.Context, s Store, x Root, p string) error {
	_, err := o.GetMetadata(ctx, s, x, p)
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
