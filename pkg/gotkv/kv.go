package gotkv

import (
	"context"

	"github.com/brendoncarroll/got/pkg/cadata"
)

type Store = cadata.Store
type ID = cadata.ID

type Node struct {
	Entries  []Entry
	Children []ChildRef
}

type ChildRef struct {
	Prefix []byte
	Ref    Ref
}

type Entry struct {
	Suffix []byte
	Value  []byte
}

func New(ctx context.Context, store Store) (*Ref, error) {
	return PostNode(ctx, store, &Node{})
}

func PostNode(ctx context.Context, store Store, n *Node) (*Ref, error) {
	panic("")
}

func GetNode(ctx context.Context, store Store, ref Ref) (*Node, error) {
	panic("")
}

// Put, puts the data from reader at offest, overwriting what's there
func Put(ctx context.Context, s Store, x Ref, key, data []byte) (*Ref, error) {
	panic("")
}

func GetF(ctx context.Context, s Store, x Ref, key []byte, fn func(data []byte) error) error {
	panic("")
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

// Prefix returns a new KV with all the keys from x prefixed with prefix
func Prefix(ctx context.Context, s Store, x Ref, prefix []byte) (*Ref, error) {
	panic("")
}
