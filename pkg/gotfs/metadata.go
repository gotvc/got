package gotfs

import (
	"context"
	"encoding/json"
	"os"

	"github.com/brendoncarroll/got/pkg/gotkv"
)

type Metadata struct {
	Mode   os.FileMode
	Labels map[string]string
}

func PutMetadata(ctx context.Context, s Store, x Ref, p string, md Metadata) (*Ref, error) {
	data, err := json.Marshal(md)
	if err != nil {
		return nil, err
	}
	return gotkv.Put(ctx, s, x, []byte(p), data)
}

func GetMetadata(ctx context.Context, s Store, x Ref, p string) (*Metadata, error) {
	panic("")
}

func checkNoEntry(ctx context.Context, s Store, x Ref, p string) error {
	_, err := GetMetadata(ctx, s, x, p)
	switch {
	case os.IsNotExist(err):
		return nil
	case err == nil:
		return os.ErrExist
	default:
		return err
	}
}
