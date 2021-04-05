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

type Metadata struct {
	Mode   os.FileMode       `json:"mode"`
	Labels map[string]string `json:"labels,omitempty"`
}

func (m Metadata) marshal() []byte {
	data, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return data
}

// PutMetadata assigne metadata to p
func PutMetadata(ctx context.Context, s Store, x Root, p string, md Metadata) (*Root, error) {
	gotkv := gotkv.NewOperator()
	return gotkv.Put(ctx, s, x, []byte(p), md.marshal())
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

func parseMetadata(data []byte) (*Metadata, error) {
	var md Metadata
	if err := json.Unmarshal(data, &md); err != nil {
		return nil, err
	}
	return &md, nil
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
