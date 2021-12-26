package gotkv

import (
	"bytes"
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/gotvc/got/pkg/gotkv/ptree"
	"github.com/pkg/errors"
)

type Builder = ptree.Builder

type Iterator = kvstreams.Iterator

type Option func(op *Operator)

func WithDataOperator(ro gdat.Operator) Option {
	return func(o *Operator) {
		o.dop = ro
	}
}

// WithMaxSize sets the max size of blobs made by the operator
func WithMaxSize(x int) Option {
	if x < 1 {
		panic(fmt.Sprint("invalid size:", x))
	}
	return func(o *Operator) {
		o.maxSize = x
	}
}

// WithAverageSize sets the average size of blobs made by the operator
func WithAverageSize(x int) Option {
	if x < 1 {
		panic(fmt.Sprint("invalid size:", x))
	}
	return func(o *Operator) {
		o.averageSize = x
	}
}

func WithSeed(seed *[32]byte) Option {
	if seed == nil {
		panic("seed cannot be nil")
	}
	return func(o *Operator) {
		o.seed = seed
	}
}

// Operator holds common configuration for operations on gotkv instances.
// It has nothing to do with the state of a particular gotkv instance. It is NOT analagous to a collection object.
// It is safe for use by multiple goroutines.
type Operator struct {
	dop                  gdat.Operator
	maxSize, averageSize int
	seed                 *[32]byte
}

func NewOperator(opts ...Option) Operator {
	op := Operator{
		dop: gdat.NewOperator(),
	}
	for _, opt := range opts {
		opt(&op)
	}
	if op.maxSize == 0 || op.averageSize == 0 {
		panic("gotkv: must set max and average size")
	}
	return op
}

func (o *Operator) GetF(ctx context.Context, s cadata.Store, x Root, key []byte, fn func([]byte) error) error {
	it := o.NewIterator(s, x, kvstreams.SingleItemSpan(key))
	var ent Entry
	err := it.Next(ctx, &ent)
	if err != nil {
		if err == kvstreams.EOS {
			err = ErrKeyNotFound
		}
		return err
	}
	return fn(ent.Value)
}

func (o *Operator) Get(ctx context.Context, s cadata.Store, x Root, key []byte) ([]byte, error) {
	var ret []byte
	if err := o.GetF(ctx, s, x, key, func(data []byte) error {
		ret = append([]byte{}, data...)
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (o *Operator) Put(ctx context.Context, s cadata.Store, x Root, key, value []byte) (*Root, error) {
	return o.Mutate(ctx, s, x, Mutation{
		Span:    SingleKeySpan(key),
		Entries: []Entry{{Key: key, Value: value}},
	})
}

func (o *Operator) Delete(ctx context.Context, s cadata.Store, x Root, key []byte) (*Root, error) {
	return o.DeleteSpan(ctx, s, x, kvstreams.SingleItemSpan(key))
}

func (o *Operator) DeleteSpan(ctx context.Context, s cadata.Store, x Root, span Span) (*Root, error) {
	return o.Mutate(ctx, s, x, Mutation{
		Span: span,
	})
}

func (o *Operator) NewEmpty(ctx context.Context, s cadata.Store) (*Root, error) {
	b := o.NewBuilder(s)
	return b.Finish(ctx)
}

func (o *Operator) MaxEntry(ctx context.Context, s cadata.Store, x Root, span Span) (*Entry, error) {
	return ptree.MaxEntry(ctx, s, x, span)
}

func (o *Operator) AddPrefix(x Root, prefix []byte) Root {
	return ptree.AddPrefix(x, prefix)
}

func (o *Operator) RemovePrefix(ctx context.Context, s cadata.Store, x Root, prefix []byte) (*Root, error) {
	return ptree.RemovePrefix(ctx, s, x, prefix)
}

func (o *Operator) NewBuilder(s Store) *Builder {
	return o.makeBuilder(s)
}

func (o *Operator) NewIterator(s Store, root Root, span Span) Iterator {
	return ptree.NewIterator(s, &o.dop, root, span)
}

func (o *Operator) makeBuilder(s cadata.Store) *ptree.Builder {
	return ptree.NewBuilder(s, &o.dop, o.averageSize, o.maxSize, o.seed)
}

func (o *Operator) ForEach(ctx context.Context, s Store, root Root, span Span, fn func(Entry) error) error {
	it := o.NewIterator(s, root, span)
	var ent Entry
	for {
		if err := it.Next(ctx, &ent); err != nil {
			if err == kvstreams.EOS {
				return nil
			}
			return err
		}
		if err := fn(ent); err != nil {
			return err
		}
	}
}

func (o *Operator) Diff(ctx context.Context, s cadata.Store, left, right Root, span Span, fn kvstreams.DiffFn) error {
	leftIt, rightIt := o.NewIterator(s, left, span), o.NewIterator(s, right, span)
	return kvstreams.Diff(ctx, s, leftIt, rightIt, span, fn)
}

// Mutation represents a declarative change to a Span of entries.
// The result of applying mutation is that
type Mutation struct {
	Span    Span
	Entries []Entry
}

// Mutate applies a batch of mutations to the tree x.
func (o *Operator) Mutate(ctx context.Context, s cadata.Store, x Root, mutations ...Mutation) (*Root, error) {
	iters := make([]kvstreams.Iterator, 2*len(mutations)+1)
	var start []byte
	for i, mut := range mutations {
		if err := checkMutation(mut); err != nil {
			return nil, err
		}
		if i > 0 {
			if bytes.Compare(mut.Span.Start, mutations[i-1].Span.End) < 0 {
				return nil, errors.Errorf("spans out of order %d start: %q < %d end: %q", i, mut.Span.Start, i-1, mut.Span.End)
			}
		}
		beforeIter := o.NewIterator(s, x, Span{
			Start: start,
			End:   append([]byte{}, mut.Span.Start...), // ensure this isn't nil, there must be an upper bound.
		})
		iters[2*i] = beforeIter
		iters[2*i+1] = kvstreams.NewLiteral(mut.Entries)
		start = mut.Span.End
	}
	iters[len(iters)-1] = o.NewIterator(s, x, Span{
		Start: start,
		End:   nil,
	})
	return o.Concat(ctx, s, iters...)
}

func checkMutation(mut Mutation) error {
	for _, ent := range mut.Entries {
		if !mut.Span.Contains(ent.Key) {
			return errors.Errorf("mutation span %v does not contain entry key %q", mut.Span, ent.Key)
		}
	}
	return nil
}

// Concat copies data from the iterators in order.
// If the iterators produce out of order keys concat errors.
func (o *Operator) Concat(ctx context.Context, s cadata.Store, iters ...kvstreams.Iterator) (*Root, error) {
	b := o.NewBuilder(s)
	for _, iter := range iters {
		if err := CopyAll(ctx, b, iter); err != nil {
			return nil, err
		}
	}
	return b.Finish(ctx)
}
