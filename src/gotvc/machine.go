package gotvc

import (
	"container/list"
	"context"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/stores"
)

type Config struct {
	Salt      [32]byte
	CacheSize *int
}

type Parser[T any] = func([]byte) (T, error)

type Machine[T Marshalable] struct {
	parse    Parser[T]
	cfg      Config
	readOnly bool
	da       *gdat.Machine
}

func NewMachine[T Marshalable](parse Parser[T], cfg Config) *Machine[T] {
	if cfg.CacheSize != nil {
		defaultCacheSize := 256
		cfg.CacheSize = &defaultCacheSize
	}
	ag := Machine[T]{
		parse: parse,
		cfg:   cfg,
	}
	ag.da = gdat.NewMachine(gdat.Params{
		Salt:      ag.cfg.Salt,
		CacheSize: cfg.CacheSize,
	})
	return &ag
}

// ForEach calls fn once for each Ref in the commit graph.
func (m *Machine[T]) ForEach(ctx context.Context, s stores.Reading, xs []Ref, fn func(Ref, Vertex[T]) error) error {
	visited := map[Ref]struct{}{}
	refs := newRefQueue()
	refs.push(xs...)
	for refs.len() > 0 {
		ref := refs.pop()
		vert, err := m.GetVertex(ctx, s, ref)
		if err != nil {
			return err
		}
		if err := fn(ref, *vert); err != nil {
			return err
		}
		visited[ref] = struct{}{}
		for _, parentRef := range vert.Parents {
			if _, exists := visited[parentRef]; !exists {
				refs.push(parentRef)
			}
		}
	}
	return nil
}

type refQueue struct {
	list *list.List
}

func newRefQueue() *refQueue {
	rq := &refQueue{
		list: list.New(),
	}
	return rq
}

func (rq *refQueue) push(xs ...Ref) {
	for _, x := range xs {
		rq.list.PushBack(x)
	}
}

func (rq *refQueue) pop() Ref {
	el := rq.list.Front()
	ret := el.Value.(Ref)
	rq.list.Remove(el)
	return ret
}

func (rq *refQueue) len() int {
	return rq.list.Len()
}
