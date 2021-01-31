package gotkv

import (
	"bytes"
	"context"
	"log"

	"github.com/pkg/errors"

	"github.com/brendoncarroll/got/pkg/gotkv/gkvproto"
)

func putRoot(ctx context.Context, s Store, x Ref, key, value []byte) (*Ref, error) {
	var y *Ref
	err := getNodeF(ctx, s, x, func(xNode Node) error {
		entries, childRefs, err := putSplit(ctx, s, x, key, value)
		if err != nil {
			return err
		}
		if len(childRefs) == 0 {
			panic("childRefs cannot go to 0 during put")
		}
		if len(childRefs) == 1 {
			ref, err := childRefs[0].Ref()
			if err != nil {
				return err
			}
			y = &ref
			return nil
		}
		n := newNode()
		tree, err := n.NewTree()
		if err != nil {
			return err
		}
		children, err := tree.NewChildren(int32(len(childRefs)))
		if err != nil {
			return err
		}
		for i := range childRefs {
			if err := children.Set(i, childRefs[i]); err != nil {
				return err
			}
		}
		if len(entries) > 0 {
			ents, err := tree.NewEntries(int32(len(entries)))
			if err != nil {
				return err
			}
			for i := range entries {
				if err := ents.Set(i, entries[i]); err != nil {
					return err
				}
			}
		}
		ref, err := postNode(ctx, s, n)
		if err != nil {
			return err
		}
		y = ref
		return nil
	})
	if err != nil {
		return nil, err
	}
	return y, nil
}

func postSplit(ctx context.Context, s Store, n Node) ([]gkvproto.Entry, []gkvproto.ChildRef, error) {
	var childRefs []gkvproto.ChildRef
	var entries []gkvproto.Entry
	if nodeSize(n) > maxNodeSize {
		var err error
		entries, childRefs, err = splitNode(ctx, s, n)
		if err != nil {
			return nil, nil, err
		}
	} else {
		ref, err := postNode(ctx, s, n)
		if err != nil {
			return nil, nil, err
		}
		childRef, err := gkvproto.NewChildRef(newSegment())
		if err != nil {
			return nil, nil, err
		}
		if err := childRef.SetRef(*ref); err != nil {
			return nil, nil, err
		}
		childRefs = []gkvproto.ChildRef{childRef}
	}
	return entries, childRefs, nil
}

func putSplit(ctx context.Context, s Store, x Ref, key, value []byte) ([]gkvproto.Entry, []gkvproto.ChildRef, error) {
	var childRefs []gkvproto.ChildRef
	var entries []gkvproto.Entry
	err := getNodeF(ctx, s, x, func(xNode Node) error {
		yNode, err := putNode(ctx, s, xNode, key, value)
		if err != nil {
			return err
		}
		entries, childRefs, err = postSplit(ctx, s, *yNode)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	return entries, childRefs, nil
}

// putNode puts a key in a node, the node returned may need to be split
func putNode(ctx context.Context, s Store, xNode Node, key, value []byte) (*Node, error) {
	yNode := newNode()
	err := func() error {
		switch xNode.Which() {
		case gkvproto.Node_Which_leaf:
			xLeaf, err := xNode.Leaf()
			if err != nil {
				return err
			}
			yLeaf, err := putLeaf(xLeaf, key, value)
			if err != nil {
				return err
			}
			if err := yNode.SetLeaf(*yLeaf); err != nil {
				return err
			}
		case gkvproto.Node_Which_tree:
			xTree, err := xNode.Tree()
			if err != nil {
				return err
			}
			yTree, err := putTree(ctx, s, xTree, key, value)
			if err != nil {
				return err
			}
			if err := yNode.SetTree(*yTree); err != nil {
				return err
			}
		default:
			xLeaf, err := xNode.NewLeaf()
			if err != nil {
				return err
			}
			yLeaf, err := putLeaf(xLeaf, key, value)
			if err != nil {
				return err
			}
			if err := yNode.SetLeaf(*yLeaf); err != nil {
				return err
			}
		}
		return nil
	}()
	if err != nil {
		return nil, err
	}
	return &yNode, nil
}

// putLeaf adds the key value pair to a leaf. The returned leaf may be too large.
func putLeaf(base gkvproto.Leaf, key, value []byte) (*gkvproto.Leaf, error) {
	y, err := gkvproto.NewLeaf(newSegment())
	if err != nil {
		return nil, err
	}
	baseEnts, err := base.Entries()
	if err != nil {
		return nil, err
	}
	// determine size of entries
	l := baseEnts.Len()
	if yes, err := entsContainKey(baseEnts, key); err != nil {
		return nil, err
	} else if !yes {
		l++
	}
	ents, err := y.NewEntries(int32(l))
	if err != nil {
		return nil, err
	}
	var i, j int
	// copy lower entries
	for ; i < baseEnts.Len(); i++ {
		ent := baseEnts.At(i)
		cmp, err := compareEntWithKey(ent, key)
		if err != nil {
			return nil, err
		}
		if cmp >= 0 {
			if cmp == 0 {
				i++
			}
			break
		}
		if err := ents.Set(j, ent); err != nil {
			return nil, err
		}
		j++
	}
	// copy entry
	newEnt, err := gkvproto.NewEntry(newSegment())
	if err != nil {
		return nil, err
	}
	newEnt.SetKey(key)
	newEnt.SetValue(value)
	if err := ents.Set(j, newEnt); err != nil {
		return nil, err
	}
	j++
	// copy upper entries
	for ; i < baseEnts.Len(); i++ {
		ent := baseEnts.At(i)
		if err := ents.Set(j, ent); err != nil {
			return nil, err
		}
		j++
	}
	return &y, nil
}

func putTree(ctx context.Context, s Store, xTree gkvproto.Tree, key, value []byte) (*gkvproto.Tree, error) {
	children, err := xTree.Children()
	if err != nil {
		return nil, err
	}
	for i := 0; i < children.Len(); i++ {
		childRef := children.At(i)
		prefix, err := childRef.Prefix()
		if err != nil {
			return nil, err
		}
		if bytes.HasPrefix(key, prefix) {
			ref, err := childRef.Ref()
			if err != nil {
				return nil, err
			}
			entries2, childRefs2, err := putSplit(ctx, s, ref, key[len(prefix):], value)
			if err != nil {
				return nil, err
			}
			log.Println(entries2, childRefs2)
			panic("not implemented")
		}
	}
	yTree, err := gkvproto.NewTree(newSegment())
	if err != nil {
		return nil, err
	}
	if len(key) == 0 {
		if err := copyChildren(xTree, yTree); err != nil {
			return nil, err
		}
		return &yTree, nil
	}
	// will call postChild here
	panic("not implemented")
}

func splitNode(ctx context.Context, s Store, x gkvproto.Node) ([]gkvproto.Entry, []gkvproto.ChildRef, error) {
	switch x.Which() {
	case gkvproto.Node_Which_leaf:
		leaf, err := x.Leaf()
		if err != nil {
			return nil, nil, err
		}
		return splitLeaf(ctx, s, leaf)

	case gkvproto.Node_Which_tree:
		tree, err := x.Tree()
		if err != nil {
			return nil, nil, err
		}
		return splitTree(ctx, s, tree)

	default:
		return nil, nil, errors.New("cannot split empty node")
	}
}

// splitEntries splits entries by the first byte of their keys
// the entry for the empty key, is also returned.
func splitEntries(xEnts gkvproto.Entry_List) ([]gkvproto.Entry, *[256][]gkvproto.Entry, error) {
	var entries []gkvproto.Entry
	var entMap [256][]gkvproto.Entry
	if xEnts.Len() < 2 {
		return nil, nil, errors.Errorf("cannot split node")
	}
	for i := 0; i < xEnts.Len(); i++ {
		xEnt := xEnts.At(i)
		key, err := xEnt.Key()
		if err != nil {
			return nil, nil, err
		}
		if len(key) == 0 {
			entries = append(entries, xEnt)
			continue
		}
		yEnt, err := gkvproto.NewEntry(newSegment())
		if err != nil {
			return nil, nil, err
		}
		value, err := xEnt.Value()
		if err != nil {
			return nil, nil, err
		}
		if err := yEnt.SetKey(key[:]); err != nil {
			return nil, nil, err
		}
		if err := yEnt.SetValue(value); err != nil {
			return nil, nil, err
		}
		c := int(key[0])
		entMap[c] = append(entMap[c], yEnt)
	}
	return entries, &entMap, nil
}

func splitLeaf(ctx context.Context, s Store, x gkvproto.Leaf) ([]gkvproto.Entry, []gkvproto.ChildRef, error) {
	xEnts, err := x.Entries()
	if err != nil {
		return nil, nil, err
	}
	entries, entMap, err := splitEntries(xEnts)
	if err != nil {
		return nil, nil, err
	}
	// collect entries into new leaves
	var ys []gkvproto.ChildRef
	for i := range entMap {
		if len(entMap[i]) > 0 {
			n := newNode()
			leaf, err := n.NewLeaf()
			if err != nil {
				return nil, nil, err
			}
			yEnts, err := leaf.NewEntries(int32(len(entMap[i])))
			if err != nil {
				return nil, nil, err
			}
			for j := range entMap[i] {
				if err := yEnts.Set(j, entMap[i][j]); err != nil {
					return nil, nil, err
				}
			}
			entries2, childRefs, err := postSplit(ctx, s, n)
			if err != nil {
				return nil, nil, err
			}
			ys = append(ys, childRefs...)
			entries = append(entries, entries2...)
		}
	}
	return entries, ys, nil
}

func splitTree(ctx context.Context, s Store, tree gkvproto.Tree) ([]gkvproto.Entry, []gkvproto.ChildRef, error) {
	panic("split tree not implemented")
}

func nodeSize(n gkvproto.Node) int {
	return int(n.Size().DataSize)
}

func copyChildren(src, dst gkvproto.Tree) error {
	srcChildren, err := src.Children()
	if err != nil {
		return err
	}
	dstChildren, err := dst.NewChildren(int32(srcChildren.Len()))
	if err != nil {
		return err
	}
	for i := 0; i < dstChildren.Len(); i++ {
		srcChildren.Set(i, dstChildren.At(i))
	}
	return nil
}
