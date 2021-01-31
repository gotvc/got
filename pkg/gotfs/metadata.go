package gotfs

import (
	"context"
	"encoding/json"
	"os"

	"github.com/brendoncarroll/got/pkg/gotkv"
)

type Metadata struct {
	Mode   os.FileMode       `json:"mode"`
	Labels map[string]string `json:"labels,omitempty"`
}

func PutMetadata(ctx context.Context, s Store, x Ref, p string, md Metadata) (*Ref, error) {
	data, err := json.Marshal(md)
	if err != nil {
		return nil, err
	}
	return gotkv.Put(ctx, s, x, []byte(p), data)
}

func GetMetadata(ctx context.Context, s Store, x Ref, p string) (*Metadata, error) {
	p = cleanPath(p)
	var md Metadata
	err := gotkv.GetF(ctx, s, x, []byte(p), func(data []byte) error {
		return json.Unmarshal(data, &md)
	})
	if err != nil {
		return nil, err
	}
	return &md, nil
}

func parseMetadata(data []byte) (*Metadata, error) {
	md := &Metadata{}
	if err := json.Unmarshal(data, md); err != nil {
		return nil, err
	}
	return md, nil
}

func checkNoEntry(ctx context.Context, s Store, x Ref, p string) error {
	_, err := GetMetadata(ctx, s, x, p)
	switch {
	case err == gotkv.ErrKeyNotFound:
		return nil
	case err == nil:
		return os.ErrExist
	default:
		return err
	}
}
