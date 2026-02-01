package gotvc

import (
	"container/list"
	"context"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/stores"
)

type Option[T Marshalable] = func(a *Machine[T])

func WithSalt[T Marshalable](salt *[32]byte) Option[T] {
	return func(a *Machine[T]) {
		a.salt = salt
	}
}

type Parser[T any] = func([]byte) (T, error)

type Machine[T Marshalable] struct {
	parse     Parser[T]
	salt      *[32]byte
	cacheSize int
	readOnly  bool
	da        *gdat.Machine
}

func NewMachine[T Marshalable](parse Parser[T], opts ...Option[T]) *Machine[T] {
	ag := Machine[T]{
		parse:     parse,
		cacheSize: 256,
		salt:      &[32]byte{},
	}
	for _, opt := range opts {
		opt(&ag)
	}
	ag.da = gdat.NewMachine(gdat.WithSalt(ag.salt), gdat.WithCacheSize(ag.cacheSize))
	return &ag
}

// ForEach calls fn once for each Ref in the snapshot graph.
func (m *Machine[T]) ForEach(ctx context.Context, s stores.Reading, xs []Ref, fn func(Ref, Vertex[T]) error) error {
	visited := map[Ref]struct{}{}
	refs := newRefQueue()
	refs.push(xs...)
	for refs.len() > 0 {
		ref := refs.pop()
		snap, err := m.GetSnapshot(ctx, s, ref)
		if err != nil {
			return err
		}
		if err := fn(ref, *snap); err != nil {
			return err
		}
		visited[ref] = struct{}{}
		for _, parentRef := range snap.Parents {
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
