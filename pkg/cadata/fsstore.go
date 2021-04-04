package cadata

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/pkg/errors"
)

type fsStore struct {
	fs fs.FS
}

func NewFSStore(x fs.FS) Store {
	return fsStore{fs: x}
}

func (s fsStore) Post(ctx context.Context, data []byte) (ID, error) {
	id := Hash(data)
	p := s.pathFromID(id)
	if err := s.fs.WriteFile(p, bytes.NewReader(data)); err != nil {
		return ID{}, err
	}
	return id, nil
}

func (s fsStore) GetF(ctx context.Context, id ID, fn func([]byte) error) error {
	p := s.pathFromID(id)
	data, err := fs.ReadFile(s.fs, p)
	if err != nil {
		return err
	}
	return fn(data)
}

func (s fsStore) Exists(ctx context.Context, id ID) (bool, error) {
	p := s.pathFromID(id)
	_, err := s.fs.Stat(p)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s fsStore) Delete(ctx context.Context, id ID) error {
	p := s.pathFromID(id)
	return s.fs.Remove(p)
}

func (s fsStore) List(ctx context.Context, prefix []byte, ids []ID) (int, error) {
	// TODO: make this more efficient
	var n int
	err := fs.WalkFiles(ctx, s.fs, "", func(p string) error {
		id, err := s.idFromPath(p)
		if err != nil {
			return err
		}
		if !bytes.HasPrefix(id[:], prefix) {
			return nil
		}
		if n >= len(ids) {
			return blobs.ErrTooMany
		}
		ids[n] = id
		n++
		return nil
	})
	if err != nil {
		return n, err
	}
	return n, nil
}

func (s fsStore) pathFromID(id ID) string {
	enc := s.encoding()
	idB64 := enc.EncodeToString(id[:])
	return filepath.Join(idB64[:1], idB64[1:])
}

func (s fsStore) idFromPath(p string) (ID, error) {
	idB64 := strings.Replace(filepath.ToSlash(p), "/", "", -1)
	enc := s.encoding()
	id := ID{}
	n, err := enc.Decode(id[:], []byte(idB64))
	if err != nil {
		return ID{}, err
	}
	if n < len(id) {
		return ID{}, errors.Errorf("not enough bytes to be ID: %q", idB64)
	}
	return id, nil
}

func (s fsStore) encoding() *base64.Encoding {
	return base64.RawURLEncoding
}
