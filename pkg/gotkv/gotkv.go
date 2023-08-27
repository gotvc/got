package gotkv

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/brendoncarroll/go-exp/streams"
	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/gotvc/got/pkg/gotkv/ptree"
	"golang.org/x/sync/errgroup"
)

type (
	Getter = cadata.Getter
	Store  = cadata.Store
	ID     = cadata.ID
	Ref    = gdat.Ref

	Entry = kvstreams.Entry
	Span  = kvstreams.Span
)

type Root struct {
	Ref   gdat.Ref `json:"ref"`
	Depth uint8    `json:"depth"`
	First []byte   `json:"first,omitempty"`
}

func newRoot(x *ptree.Root[Entry, gdat.Ref]) *Root {
	if x == nil {
		return nil
	}
	lb, _ := x.Span.LowerBound()
	return &Root{
		Ref:   x.Ref,
		Depth: x.Depth,
		First: lb.Key,
	}
}

func (r Root) toPtree() ptree.Root[Entry, Ref] {
	span := state.TotalSpan[Entry]()
	span = span.WithLowerIncl(Entry{Key: r.First})

	return ptree.Root[Entry, Ref]{
		Index: ptree.Index[Entry, Ref]{
			Ref:       r.Ref,
			Span:      span,
			IsNatural: false,
		},
		Depth: r.Depth,
	}
}

const (
	MaxKeySize = 4096
)

var (
	ErrKeyNotFound = fmt.Errorf("key not found")
)

var defaultReadOnlyAgent = &Agent{da: gdat.NewAgent()}

// Get is a convenience function for performing Get without creating an Agent.
func Get(ctx context.Context, s Getter, x Root, key []byte) ([]byte, error) {
	return defaultReadOnlyAgent.Get(ctx, s, x, key)
}

// GetF is a convenience function for performing GetF without creating an Agent
func GetF(ctx context.Context, s Getter, x Root, key []byte, fn func([]byte) error) error {
	return defaultReadOnlyAgent.GetF(ctx, s, x, key, fn)
}

// CopyAll copies all the entries from iterator to builder.
func CopyAll(ctx context.Context, b *Builder, it kvstreams.Iterator) error {
	if it, ok := it.(*Iterator); ok {
		return ptree.Copy(ctx, &b.b, &it.it)
	}
	return streams.ForEach[Entry](ctx, it, func(ent Entry) error {
		return b.Put(ctx, ent.Key, ent.Value)
	})
}

// Sync ensures dst has all the data reachable from x.
func (a *Agent) Sync(ctx context.Context, src cadata.Getter, dst Store, x Root, entryFn func(Entry) error) error {
	rp := ptree.ReadParams[Entry, Ref]{
		Compare:         compareEntries,
		Store:           &ptreeGetter{ag: a.da, s: src},
		NewIndexDecoder: func() ptree.IndexDecoder[Entry, Ref] { return &IndexDecoder{} },
		NewDecoder:      func() ptree.Decoder[Entry, Ref] { return &Decoder{} },
	}
	return do(ctx, rp, x.toPtree(), doer{
		CanSkip: func(r Root) (bool, error) {
			return dst.Exists(ctx, r.Ref.CID)
		},
		EntryFn: entryFn,
		NodeFn: func(r Root) error {
			return gdat.Copy(ctx, src, dst, &r.Ref)
		},
	})
}

// Populate adds all blobs reachable from x to set.
// If an item is in set all of the blobs reachable from it are also assumed to also be in set.
func (a *Agent) Populate(ctx context.Context, s Store, x Root, set cadata.Set, entryFn func(ent Entry) error) error {
	rp := ptree.ReadParams[Entry, Ref]{
		Compare:    compareEntries,
		Store:      &ptreeGetter{ag: a.da, s: s},
		NewDecoder: func() ptree.Decoder[Entry, Ref] { return &Decoder{} },
	}
	return do(ctx, rp, x.toPtree(), doer{
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

func do(ctx context.Context, rp ptree.ReadParams[Entry, Ref], x ptree.Root[Entry, Ref], p doer) error {
	if canSkip, err := p.CanSkip(*(newRoot(&x))); err != nil {
		return err
	} else if canSkip {
		return nil
	}
	if ptree.PointsToEntries(x) {
		ents, err := ptree.ListEntries(ctx, rp, x.Index)
		if err != nil {
			return err
		}
		for _, ent := range ents {
			if err := p.EntryFn(ent); err != nil {
				return err
			}
		}
	} else {
		idxs, err := ptree.ListIndexes(ctx, rp, x)
		if err != nil {
			return err
		}
		eg, ctx := errgroup.WithContext(ctx)
		for _, idx := range idxs {
			root2 := ptree.Root[Entry, Ref]{
				Index: idx,
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
	return p.NodeFn(*newRoot(&x))
}

type ptreeGetter struct {
	ag *gdat.Agent
	s  cadata.Getter
}

func (s *ptreeGetter) Get(ctx context.Context, ref Ref, buf []byte) (int, error) {
	return s.ag.Read(ctx, s.s, ref, buf)
}

func (s *ptreeGetter) MaxSize() int {
	return s.s.MaxSize()
}

type ptreeStore struct {
	ag *gdat.Agent
	s  cadata.Store
}

func (s *ptreeStore) Post(ctx context.Context, data []byte) (Ref, error) {
	ref, err := s.ag.Post(ctx, s.s, data)
	if err != nil {
		return Ref{}, err
	}
	return *ref, nil
}

func (s *ptreeStore) Get(ctx context.Context, ref Ref, buf []byte) (int, error) {
	return s.ag.Read(ctx, s.s, ref, buf)
}

func (s *ptreeStore) MaxSize() int {
	return s.s.MaxSize()
}

// DebugTree writes human-readable debug information about the tree to w.
func DebugTree(ctx context.Context, s cadata.Store, root Root, w io.Writer) error {
	rp := ptree.ReadParams[Entry, Ref]{
		Store:           &ptreeGetter{s: s, ag: defaultReadOnlyAgent.da},
		Compare:         compareEntries,
		NewDecoder:      func() ptree.Decoder[Entry, Ref] { return &Decoder{} },
		NewIndexDecoder: func() ptree.IndexDecoder[Entry, Ref] { return &IndexDecoder{} },
	}
	return ptree.DebugTree(ctx, rp, root.toPtree(), w)
}

func compareEntries(a, b Entry) int {
	return bytes.Compare(a.Key, b.Key)
}

func copyEntry(dst *Entry, src Entry) {
	kvstreams.CopyEntry(dst, src)
}

func convertSpan(x kvstreams.Span) state.Span[Entry] {
	y := state.TotalSpan[Entry]()
	if x.Begin != nil {
		y = y.WithLowerIncl(Entry{Key: x.Begin})
	}
	if x.End != nil {
		y = y.WithUpperExcl(Entry{Key: x.End})
	}
	return y
}
