package stores

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/fs"
	"github.com/pkg/errors"
)

type fsStore struct {
	fs      fs.FS
	maxSize int
}

func NewFSStore(x fs.FS, maxSize int) Store {
	return fsStore{fs: x, maxSize: maxSize}
}

func (s fsStore) Post(ctx context.Context, data []byte) (ID, error) {
	if len(data) > s.maxSize {
		return ID{}, cadata.ErrTooLarge
	}
	id := cadata.DefaultHash(data)
	p := s.pathFromID(id)
	if err := s.fs.WriteFile(p, bytes.NewReader(data)); err != nil {
		return ID{}, err
	}
	return id, nil
}

func (s fsStore) Read(ctx context.Context, id ID, buf []byte) (int, error) {
	p := s.pathFromID(id)
	f, err := s.fs.Open(p)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return copyToBuffer(buf, f)
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
			return cadata.ErrTooMany
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

func (s fsStore) Hash(x []byte) ID {
	return cadata.DefaultHash(x)
}

func (s fsStore) MaxSize() int {
	return s.maxSize
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

func copyToBuffer(dst []byte, r io.Reader) (int, error) {
	bw := newByteWriter(dst)
	n, err := io.Copy(bw, r)
	return int(n), err
}

type byteWriter struct {
	n   int
	buf []byte
}

func newByteWriter(buf []byte) *byteWriter {
	return &byteWriter{buf: buf}
}

func (bw *byteWriter) Write(p []byte) (int, error) {
	n := copy(bw.buf[bw.n:], p)
	if n < len(p) {
		return n, io.ErrShortBuffer
	}
	bw.n += n
	return n, nil
}
