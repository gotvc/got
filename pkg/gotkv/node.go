package gotkv

import (
	"bytes"
	"context"

	capnp "zombiezen.com/go/capnproto2"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gotkv/gkvproto"
	"github.com/pkg/errors"
)

type Node = gkvproto.Node

const maxNodeSize = 2 << 15

func newSegment() *capnp.Segment {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		panic(err)
	}
	return seg
}

func newChildNode() Node {
	n := newNode()
	_, err := n.NewChild()
	if err != nil {
		panic(err)
	}
	return n
}

func newNode() Node {
	seg := newSegment()
	n, err := gkvproto.NewRootNode(seg)
	if err != nil {
		panic(err)
	}
	return n
}

func postNode(ctx context.Context, store Store, n Node) (*Ref, error) {
	buf := bytes.NewBuffer(nil)
	if err := capnp.NewEncoder(buf).Encode(n.Segment().Message()); err != nil {
		return nil, err
	}
	return PostRaw(ctx, store, buf.Bytes())
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

func getF(ctx context.Context, s Store, x Ref, key []byte, fn func([]byte) error) error {
	return getNodeF(ctx, s, x, func(n Node) error {
		switch n.Which() {
		case gkvproto.Node_Which_child:
			child, err := n.Child()
			if err != nil {
				return err
			}
			ents, err := child.Entries()
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
				}
			}
			return ErrKeyNotFound
		case gkvproto.Node_Which_parent:
			par, err := n.Parent()
			if err != nil {
				return err
			}
			ent, err := par.Entry()
			if err != nil {
				return err
			}
			c, err := compareEntWithKey(ent, key)
			if err != nil {
				return err
			}
			if c == 0 {
				v, err := ent.Value()
				if err != nil {
					return err
				}
				return fn(v)
			}
			return ErrKeyNotFound
		default:
			return errInvalidNode()
		}
	})
}

func put(ctx context.Context, s Store, x Ref, key, value []byte) (*Ref, error) {
	var y *Ref
	err := getNodeF(ctx, s, x, func(n Node) error {
		n2 := newNode()
		switch n.Which() {
		case gkvproto.Node_Which_child:
			curChild, err := n.Child()
			if err != nil {
				return err
			}
			newChild, err := n2.NewChild()
			if err != nil {
				return err
			}
			if err := childPut(curChild, newChild, key, value); err != nil {
				return err
			}
			ref, err := postNode(ctx, s, n2)
			if err != nil {
				return err
			}
			y = ref
			return nil
		case gkvproto.Node_Which_parent:
			panic("not implemented")

		default:
			n2 := newNode()
			curChild, err := n.NewChild()
			if err != nil {
				return err
			}
			newChild, err := n2.NewChild()
			if err != nil {
				return err
			}
			if err := childPut(curChild, newChild, key, value); err != nil {
				return err
			}
			ref, err := postNode(ctx, s, n2)
			if err != nil {
				return err
			}
			y = ref
			return nil
		}
	})
	if err != nil {
		return nil, err
	}
	return y, nil
}

func childPut(base, n gkvproto.Child, key, value []byte) error {
	baseEnts, err := base.Entries()
	if err != nil {
		return err
	}
	// determine size of entries
	l := baseEnts.Len()
	if yes, err := entsContainKey(baseEnts, key); err != nil {
		return err
	} else if !yes {
		l++
	}
	ents, err := n.NewEntries(int32(l))
	if err != nil {
		return err
	}
	var i, j int
	// copy lower entries
	for ; i < baseEnts.Len(); i++ {
		ent := baseEnts.At(i)
		cmp, err := compareEntWithKey(ent, key)
		if err != nil {
			return err
		}
		if cmp >= 0 {
			if cmp == 0 {
				i++
			}
			break
		}
		if err := ents.Set(j, ent); err != nil {
			return err
		}
		j++
	}
	// copy entry
	newEnt, err := gkvproto.NewEntry(newSegment())
	if err != nil {
		return err
	}
	newEnt.SetKey(key)
	newEnt.SetValue(value)
	if err := ents.Set(j, newEnt); err != nil {
		return err
	}
	j++
	// copy upper entries
	for ; i < baseEnts.Len(); i++ {
		ent := baseEnts.At(i)
		if err := ents.Set(j, ent); err != nil {
			return err
		}
	}
	return nil
}

func compareEntWithKey(ent gkvproto.Entry, key []byte) (int, error) {
	entKey, err := ent.Key()
	if err != nil {
		return 0, err
	}
	return bytes.Compare(entKey, key), nil
}

func entsContainKey(ents gkvproto.Entry_List, key []byte) (bool, error) {
	for i := 0; i < ents.Len(); i++ {
		entKey, err := ents.At(i).Key()
		if err != nil {
			return false, err
		}
		if bytes.Equal(entKey, key) {
			return true, nil
		}
	}
	return false, nil
}

func splitChild(child gkvproto.Child) (left, right *gkvproto.Child, err error) {
	panic("")
}

func nodeForEach(ctx context.Context, s cadata.Store, n Node, fn func(k, v []byte) error) error {
	switch n.Which() {
	case gkvproto.Node_Which_child:
		child, err := n.Child()
		if err != nil {
			return err
		}
		return childForEach(child, fn)
	case gkvproto.Node_Which_parent:
		panic("")
	default:
		return errInvalidNode()
	}
}

func childForEach(x gkvproto.Child, fn func(k, v []byte) error) error {
	ents, err := x.Entries()
	if err != nil {
		return err
	}
	for i := 0; i < ents.Len(); i++ {
		ent := ents.At(i)
		k, err := ent.Key()
		if err != nil {
			return err
		}
		v, err := ent.Value()
		if err != nil {
			return err
		}
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}
