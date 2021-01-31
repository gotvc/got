package gotkv

import (
	"context"
	"errors"

	"github.com/brendoncarroll/got/pkg/cadata"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

func errInvalidNode() error {
	return errors.New("invalid node")
}

type Store = cadata.Store
type ID = cadata.ID

func New(ctx context.Context, store Store) (*Ref, error) {
	n := newNode()
	return postNode(ctx, store, n)
}

// Put adds an entry for key -> value overwriting what's there
func Put(ctx context.Context, s Store, x Ref, key, value []byte) (*Ref, error) {
	return putRoot(ctx, s, x, key, value)
}

func GetF(ctx context.Context, s Store, x Ref, key []byte, fn func(data []byte) error) error {
	return getF(ctx, s, x, key, fn)
}

func Get(ctx context.Context, s Store, x Ref, key []byte) (ret []byte, err error) {
	err = GetF(ctx, s, x, key, func(v []byte) error {
		if v != nil {
			ret = append([]byte{}, v...)
		}
		return nil
	})
	return ret, err
}

func MaxKey(ctx context.Context, s Store, x Ref, prefix []byte) ([]byte, error) {
	panic("")
}

// Delete deletes the key at k
func Delete(ctx context.Context, s Store, x Ref, k []byte) (*Ref, error) {
	panic("")
}

// DeletePrefix deletes every key that starts with prefix
func DeletePrefix(ctx context.Context, s Store, x Ref, prefix []byte) (*Ref, error) {
	panic("")
}

// AddPrefix returns a new KV with all the keys from x prefixed with prefix
func AddPrefix(ctx context.Context, s Store, x Ref, prefix []byte) (*Ref, error) {
	panic("")
}
