package gotfs

import (
	"context"
	"os"
	"strings"

	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

const Sep = "/"

func GetDirMetadata(ctx context.Context, s Store, x Ref, p string) (*Metadata, error) {
	md, err := GetMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !md.Mode.IsDir() {
		return nil, errors.Errorf("%s is not a directory", p)
	}
	return md, nil
}

type DirEnt struct {
	Name string
	Mode os.FileMode
}

func Mkdir(ctx context.Context, s Store, x Ref, p string) (*Ref, error) {
	if err := checkNoEntry(ctx, s, x, p); err != nil {
		return nil, err
	}
	md := Metadata{
		Mode: 0o755 | os.ModeDir,
	}
	return PutMetadata(ctx, s, x, p, md)
}

func EnsureDir(ctx context.Context, s Store, x Ref, p string) (*Ref, error) {
	parts := strings.Split(p, Sep)
	for i := range parts {
		p2 := strings.Join(parts[:i+1], Sep)
		_, err := GetDirMetadata(ctx, s, x, p2)
		if os.IsNotExist(err) {
			x2, err := Mkdir(ctx, s, x, parts[0])
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

func ReadDir(ctx context.Context, s Store, x Ref, fn func(e DirEnt) error) error {
	panic("")
}

func RemoveAll(ctx context.Context, s Store, x Ref, p string) (*Ref, error) {
	return gotkv.DeletePrefix(ctx, s, x, []byte(p))
}

func Merge(ctx context.Context, s Store, xs []Ref) (*Ref, error) {
	return gotkv.Reduce(ctx, s, xs, func(key []byte, v1, v2 []byte) ([]byte, error) {
		panic("")
	})
}
