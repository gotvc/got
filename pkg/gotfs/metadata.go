package gotfs

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

const MaxPathLen = 4096

type Object struct {
	Metadata *Metadata `json:"md,omitempty"`
	Part     *Part     `json:"part,omitempty"`
}

type Metadata struct {
	Mode   os.FileMode       `json:"mode"`
	Labels map[string]string `json:"labels,omitempty"`
}

// PutMetadata assigne metadata to p
func PutMetadata(ctx context.Context, s Store, x Root, p string, md Metadata) (*Root, error) {
	o := Object{Metadata: &md}
	data, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	gotkv := gotkv.NewOperator()
	return gotkv.Put(ctx, s, x, []byte(p), data)
}

// GetMetadata retrieves the metadata at p if it exists and errors otherwise
func GetMetadata(ctx context.Context, s Store, x Root, p string) (*Metadata, error) {
	p = cleanPath(p)
	var md *Metadata
	op := gotkv.NewOperator()
	err := op.GetF(ctx, s, x, []byte(p), func(data []byte) error {
		var err error
		md, err = parseMetadata(data)
		return err
	})
	if err != nil {
		return nil, err
	}
	return md, nil
}

// GetDirMetadata returns directory metadata at p if it exists, and errors otherwise
func GetDirMetadata(ctx context.Context, s Store, x Root, p string) (*Metadata, error) {
	md, err := GetMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	if !md.Mode.IsDir() {
		return nil, errors.Errorf("%s is not a directory", p)
	}
	return md, nil
}

func marshalObject(o *Object) []byte {
	data, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}
	return data
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

func checkNoEntry(ctx context.Context, s Store, x Root, p string) error {
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

func checkPath(p string) error {
	if len(p) > MaxPathLen {
		return errors.Errorf("path too long: %q", p)
	}
	if strings.ContainsAny(p, "\x00") {
		return errors.Errorf("path cannot contain null")
	}
	return nil
}
