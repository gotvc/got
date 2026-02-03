package gotvc

import (
	"container/list"
	"context"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/internal/stores"
)

type Config struct {
	Salt [32]byte
}

type Parser[T any] = func([]byte) (T, error)

type Machine[T Marshalable] struct {
	parse     Parser[T]
	cfg       Config
	cacheSize int
	readOnly  bool
	da        *gdat.Machine
}

func NewMachine[T Marshalable](parse Parser[T], cfg Config) *Machine[T] {
	ag := Machine[T]{
		parse:     parse,
		cfg:       cfg,
		cacheSize: 256,
	}
	ag.da = gdat.NewMachine(gdat.WithSalt(&ag.cfg.Salt), gdat.WithCacheSize(ag.cacheSize))
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
