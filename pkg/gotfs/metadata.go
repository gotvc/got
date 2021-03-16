package gotfs

import (
	"context"
	"encoding/json"
	"os"

	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

type Object struct {
	Metadata *Metadata `json:"md,omitempty"`
	Part     *Part     `json:"part,omitempty"`
}

type Metadata struct {
	Mode   os.FileMode       `json:"mode"`
	Labels map[string]string `json:"labels,omitempty"`
}

func PutMetadata(ctx context.Context, s Store, x Ref, p string, md Metadata) (*Ref, error) {
	o := Object{Metadata: &md}
	data, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	return gotkv.Put(ctx, s, x, []byte(p), data)
}

func GetMetadata(ctx context.Context, s Store, x Ref, p string) (*Metadata, error) {
	p = cleanPath(p)
	var md *Metadata
	err := gotkv.GetF(ctx, s, x, []byte(p), func(data []byte) error {
		var err error
		md, err = parseMetadata(data)
		return err
	})
	if err != nil {
		return nil, err
	}
	return md, nil
}

func parseObject(data []byte) (*Object, error) {
	var o Object
	if err := json.Unmarshal(data, &o); err != nil {
		return nil, err
	}
	return &o, nil
}

func parseMetadata(data []byte) (*Metadata, error) {
	o, err := parseObject(data)
	if err != nil {
		return nil, err
	}
	if o.Metadata == nil {
		return nil, errors.Errorf("object does not contain metadata")
	}
	return o.Metadata, nil
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
