package gotvc

import (
	"container/list"
	"context"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/stores"
)

type Params[T any] struct {
	Parse Parser[T]
	// Data are the parameters used for storing data in the store.
	Data gdat.Params
}

type Parser[T any] = func([]byte) (T, error)

type Machine[T Marshalable] struct {
	parse    Parser[T]
	cfg      Params[T]
	readOnly bool
	da       *gdat.Machine
}

func NewMachine[T Marshalable](p Params[T]) Machine[T] {
	if p.Data.CacheSize != nil {
		defaultCacheSize := 256
		p.Data.CacheSize = &defaultCacheSize
	}
	m := Machine[T]{
		parse: p.Parse,
		cfg:   p,
	}
	m.da = gdat.NewMachine(p.Data)
	return m
}

// ForEach calls fn once for each Ref in the commit graph.
func (m *Machine[T]) ForEach(ctx context.Context, s stores.RO, xs []Ref, fn func(Ref, Vertex[T]) error) error {
	visited := map[Ref]struct{}{}
	refs := newRefQueue()
	refs.push(xs...)
	for refs.len() > 0 {
		ref := refs.pop()
		vert, err := m.GetVertex(ctx, s, ref)
		if err != nil {
			return err
		}
		if err := fn(ref, vert); err != nil {
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
