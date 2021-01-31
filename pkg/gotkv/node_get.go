package gotkv

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	capnp "zombiezen.com/go/capnproto2"

	"github.com/brendoncarroll/got/pkg/gotkv/gkvproto"
)

func getF(ctx context.Context, s Store, x Ref, key []byte, fn func([]byte) error) error {
	return getNodeF(ctx, s, x, func(n Node) (retErr error) {
		switch n.Which() {
		case gkvproto.Node_Which_leaf:
			leaf, err := n.Leaf()
			if err != nil {
				return err
			}
			return leafGetF(leaf, key, fn)

		case gkvproto.Node_Which_tree:
			tree, err := n.Tree()
			if err != nil {
				return err
			}
			return treeGetF(ctx, s, tree, key, fn)

		default:
			return ErrKeyNotFound
		}
	})
}

func getNodeF(ctx context.Context, s Store, ref Ref, fn func(Node) error) error {
	return GetRawF(ctx, s, ref, func(data []byte) error {
		msg, err := capnp.Unmarshal(data)
		if err != nil {
			return errors.Wrap(err, "during unmarshal")
		}
		n, err := gkvproto.ReadRootNode(msg)
		if err != nil {
			return err
		}
		return fn(n)
	})
}

func leafGetF(leaf gkvproto.Leaf, key []byte, fn func(data []byte) error) error {
	ents, err := leaf.Entries()
	if err != nil {
		return err
	}
	for i := 0; i < ents.Len(); i++ {
		ent := ents.At(i)
		if c, err := compareEntWithKey(ent, key); err != nil {
			return err
		} else if c == 0 {
			entValue, err := ent.Value()
			if err != nil {
				return err
			}
			return fn(entValue)
		} else if c > 0 {
			break
		}
	}
	return ErrKeyNotFound
}

func treeGetF(ctx context.Context, s Store, tree gkvproto.Tree, key []byte, fn func(data []byte) error) error {
	if tree.HasEntries() {
		ents, err := tree.Entries()
		if err != nil {
			return err
		}
		for i := 0; i < ents.Len(); i++ {
			ent := ents.At(i)
			c, err := compareEntWithKey(ent, key)
			if err != nil {
				return err
			}
			if c == 0 {
				value, err := ent.Value()
				if err != nil {
					return err
				}
				return fn(value)
			}
			if c > 0 {
				break
			}
		}
	}
	childRefs, err := tree.Children()
	if err != nil {
		return err
	}
	for i := 0; i < childRefs.Len(); i++ {
		childRef := childRefs.At(i)
		prefix, err := childRef.Prefix()
		if err != nil {
			return err
		}
		if bytes.HasPrefix(key, prefix) {
			ref, err := childRef.Ref()
			if err != nil {
				return err
			}
			return getF(ctx, s, ref, key[len(prefix):], fn)
		}
	}
	return ErrKeyNotFound
}
