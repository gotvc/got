package gotkv

import (
	"bytes"
	"context"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/gotvc/got/pkg/gotkv/ptree"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type (
	Getter = cadata.Getter
	Store  = cadata.Store
	ID     = cadata.ID
	Ref    = gdat.Ref

	Entry = kvstreams.Entry
	Span  = kvstreams.Span

	Root = ptree.Root[Ref]
)

const (
	MaxKeySize = ptree.MaxKeySize
)

var (
	ErrKeyNotFound = errors.Errorf("key not found")
	EOS            = kvstreams.EOS
)

var defaultReadOnlyOperator = &Operator{dop: gdat.NewOperator()}

// Get is a convenience function for performing Get without creating an Operator.
func Get(ctx context.Context, s Getter, x Root, key []byte) ([]byte, error) {
	return defaultReadOnlyOperator.Get(ctx, s, x, key)
}

// GetF is a convenience function for performing GetF without creating an Operator
func GetF(ctx context.Context, s Getter, x Root, key []byte, fn func([]byte) error) error {
	return defaultReadOnlyOperator.GetF(ctx, s, x, key, fn)
}

// CopyAll copies all the entries from iterator to builder.
func CopyAll(ctx context.Context, b *Builder, it kvstreams.Iterator) error {
	if pti, ok := it.(*ptree.Iterator[Ref]); ok {
		return ptree.Copy(ctx, b, pti)
	}
	return kvstreams.ForEach(ctx, it, func(ent Entry) error {
		return b.Put(ctx, ent.Key, ent.Value)
	})
}

// Sync ensures dst has all the data reachable from x.
func (o *Operator) Sync(ctx context.Context, src cadata.Getter, dst Store, x Root, entryFn func(Entry) error) error {
	rp := ptree.ReadParams[Ref]{
		Compare:    bytes.Compare,
		Store:      &ptreeGetter{op: &o.dop, s: src},
		ParseRef:   gdat.ParseRef,
		NewDecoder: func() ptree.Decoder { return &Decoder{} },
	}
	return do(ctx, rp, x, doer{
		CanSkip: func(r Root) (bool, error) {
			return cadata.Exists(ctx, dst, r.Ref.CID)
		},
		EntryFn: entryFn,
		NodeFn: func(r Root) error {
			return gdat.Copy(ctx, src, dst, &r.Ref)
		},
	})
}

// Populate adds all blobs reachable from x to set.
// If an item is in set all of the blobs reachable from it are also assumed to also be in set.
func (o *Operator) Populate(ctx context.Context, s Store, x Root, set cadata.Set, entryFn func(ent Entry) error) error {
	rp := ptree.ReadParams[Ref]{
		Compare:    bytes.Compare,
		Store:      &ptreeGetter{op: &o.dop, s: s},
		ParseRef:   gdat.ParseRef,
		NewDecoder: func() ptree.Decoder { return &Decoder{} },
	}
	return do(ctx, rp, x, doer{
		CanSkip: func(r Root) (bool, error) {
			return set.Exists(ctx, r.Ref.CID)
		},
		EntryFn: entryFn,
		NodeFn: func(r Root) error {
			return set.Add(ctx, r.Ref.CID)
		},
	})
}

type doer struct {
	// CanSkip is called before processing each node.
	// CanSkip should return true if the node can be skipped
	CanSkip func(r Root) (bool, error)
	// EntryFn is called for each Entry
	EntryFn func(ent Entry) error
	// NodeFn is called after an entire node has been handled
	NodeFn func(r Root) error
}

func do(ctx context.Context, rp ptree.ReadParams[Ref], x Root, p doer) error {
	if canSkip, err := p.CanSkip(x); err != nil {
		return err
	} else if canSkip {
		return nil
	}
	if ptree.PointsToEntries(x) {
		ents, err := ptree.ListEntries(ctx, rp, ptree.Index[Ref]{First: x.First, Ref: x.Ref})
		if err != nil {
			return err
		}
		for _, ent := range ents {
			if err := p.EntryFn(ent); err != nil {
				return err
			}
		}
	} else {
		idxs, err := ptree.ListChildren(ctx, rp, x)
		if err != nil {
			return err
		}
		eg, ctx := errgroup.WithContext(ctx)
		for _, idx := range idxs {
			root2 := Root{
				Ref:   idx.Ref,
				First: idx.First,
				Depth: x.Depth - 1,
			}
			eg.Go(func() error {
				return do(ctx, rp, root2, p)
			})
		}
		if err := eg.Wait(); err != nil {
			return err
		}
	}
	return p.NodeFn(x)
}

type ptreeGetter struct {
	op *gdat.Operator
	s  cadata.Getter
}

func (s *ptreeGetter) Get(ctx context.Context, ref Ref, buf []byte) (int, error) {
	return s.op.Read(ctx, s.s, ref, buf)
}

func (s *ptreeGetter) MaxSize() int {
	return s.s.MaxSize()
}

type ptreeStore struct {
	op *gdat.Operator
	s  cadata.Store
}

func (s *ptreeStore) Post(ctx context.Context, data []byte) (Ref, error) {
	ref, err := s.op.Post(ctx, s.s, data)
	if err != nil {
		return Ref{}, err
	}
	return *ref, nil
}

func (s *ptreeStore) Get(ctx context.Context, ref Ref, buf []byte) (int, error) {
	return s.op.Read(ctx, s.s, ref, buf)
}

func (s *ptreeStore) MaxSize() int {
	return s.s.MaxSize()
}

// DebugTree writes human-readable debug information about the tree to w.
func DebugTree(ctx context.Context, s cadata.Store, root Root, w io.Writer) error {
	rp := ptree.ReadParams[Ref]{
		Store:      &ptreeGetter{s: s, op: &defaultReadOnlyOperator.dop},
		Compare:    bytes.Compare,
		ParseRef:   gdat.ParseRef,
		NewDecoder: func() ptree.Decoder { return &Decoder{} },
	}
	return ptree.DebugTree(ctx, rp, root, w)
}
