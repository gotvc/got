package gotkv

import (
	"bytes"
	"context"

	capnp "zombiezen.com/go/capnproto2"

	"github.com/brendoncarroll/got/pkg/gotkv/gkvproto"
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

func newNode() Node {
	seg := newSegment()
	n, err := gkvproto.NewRootNode(seg)
	if err != nil {
		panic(err)
	}
	return n
}

// postNode attempts to post a node, without splitting.
func postNode(ctx context.Context, store Store, n Node) (*Ref, error) {
	buf := bytes.NewBuffer(nil)
	if err := capnp.NewEncoder(buf).Encode(n.Segment().Message()); err != nil {
		return nil, err
	}
	return PostRaw(ctx, store, buf.Bytes())
}

func nodeLCP(n Node) ([]byte, error) {
	var lcp []byte
	switch n.Which() {
	case gkvproto.Node_Which_leaf:
		leaf, err := n.Leaf()
		if err != nil {
			return nil, err
		}
		ents, err := leaf.Entries()
		if err != nil {
			return nil, err
		}
		err = forEachEntries(ents, func(k, _ []byte) error {
			lcp = longestCommonPrefix(lcp, k)
			return nil
		})
		if err != nil {
			return nil, err
		}
	case gkvproto.Node_Which_tree:
		tree, err := n.Tree()
		if err != nil {
			return nil, err
		}
		children, err := tree.Children()
		if err != nil {
			return nil, err
		}
		for i := 0; i < children.Len(); i++ {
			k, err := children.At(i).Prefix()
			if err != nil {
				return nil, err
			}
			lcp = longestCommonPrefix(lcp, k)
		}
	}
	return lcp, nil
}

func longestCommonPrefix(lcp, x []byte) []byte {
	if lcp == nil {
		return x
	}
	l := len(lcp)
	if len(x) < l {
		l = len(x)
	}
	for i := 0; i < l; i++ {
		if lcp[i] != x[i] {
			return lcp[:i]
		}
	}
	return []byte{}
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
