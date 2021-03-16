package gotkv

import (
	"bytes"
	"context"
	"io"

	"github.com/brendoncarroll/got/pkg/gotkv/gkvproto"
)

type Iterator struct {
	ctx     context.Context
	s       Store
	root    Ref
	lastKey []byte
}

func NewIterator(ctx context.Context, s Store, x Ref) *Iterator {
	return &Iterator{
		ctx:  ctx,
		s:    s,
		root: x,
	}
}

func (iter *Iterator) SeekPast(key []byte) {
	iter.lastKey = append([]byte{}, key...)
}

func (iter *Iterator) Next(fn func(key, value []byte) error) error {
	if iter.lastKey == nil {
		err := GetF(iter.ctx, iter.s, iter.root, nil, func(data []byte) error {
			return fn([]byte{}, data)
		})
		if err != nil && err != ErrKeyNotFound {
			return err
		} else if err != nil {
			// if wasn't called
			return iter.Next(fn)
		} else {
			// it was called during GetF so return
			iter.lastKey = []byte{}
			return nil
		}
	}
	key, err := NextAfter(iter.ctx, iter.s, iter.root, iter.lastKey)
	if err != nil {
		return err
	}
	if err := GetF(iter.ctx, iter.s, iter.root, key, func(value []byte) error {
		return fn(key, value)
	}); err != nil {
		return err
	}
	iter.lastKey = key
	return nil
}

func (iter *Iterator) LastKey() []byte {
	return iter.lastKey
}

var ErrNextNotFound = io.EOF

func NextAfter(ctx context.Context, s Store, x Ref, k []byte) ([]byte, error) {
	var ret []byte
	if err := getNodeF(ctx, s, x, func(n Node) error {
		switch n.Which() {
		case gkvproto.Node_Which_tree:
			tree, err := n.Tree()
			if err != nil {
				return err
			}
			ents, err := tree.Entries()
			if err != nil {
				return err
			}
			if ents.Len() > 0 {
				ent := ents.At(0)
				key, err := ent.Key()
				if err != nil {
					return err
				}
				ret = append([]byte{}, key...)
			}
			children, err := tree.Children()
			if err != nil {
				return err
			}
			for i := 0; i < children.Len(); i++ {
				child := children.At(i)
				prefix, err := child.Prefix()
				if err != nil {
					return err
				}
				x2, err := child.Ref()
				if err != nil {
					return err
				}
				if k == nil || bytes.Compare(prefix, k) > 0 {
					ret, err = NextAfter(ctx, s, x2, k)
					return err
				}
				if bytes.HasPrefix(k, prefix) {
					keyAfter, err := NextAfter(ctx, s, x2, k[len(prefix):])
					if err == nil {
						ret = append([]byte{}, prefix...)
						ret = append(ret, keyAfter...)
						return nil
					} else if err != ErrNextNotFound {
						return err
					}
				}
			}
			return ErrNextNotFound
		case gkvproto.Node_Which_leaf:
			leaf, err := n.Leaf()
			if err != nil {
				return err
			}
			ents, err := leaf.Entries()
			if err != nil {
				return err
			}
			for i := 0; i < ents.Len(); i++ {
				ent := ents.At(i)
				entKey, err := ent.Key()
				if err != nil {
					return err
				}
				if k == nil || bytes.Compare(entKey, k) > 0 {
					ret = entKey
					return nil
				}
			}
			return ErrNextNotFound
		default:
			return errInvalidNode()
		}
	}); err != nil {
		return nil, err
	}
	return ret, nil
}
