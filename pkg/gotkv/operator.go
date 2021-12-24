package gotkv

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/gotvc/got/pkg/gotkv/ptree"
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

func (o *Operator) Put(ctx context.Context, s cadata.Store, x Root, key, value []byte) (*Root, error) {
	b := o.makeBuilder(s)
	beforeIter := o.NewIterator(s, x, Span{End: key})
	afterIter := o.NewIterator(s, x, Span{Start: KeyAfter(key)})
	if err := CopyAll(ctx, b, beforeIter); err != nil {
		return nil, err
	}
	if err := b.Put(ctx, key, value); err != nil {
		return nil, err
	}
	if err := CopyAll(ctx, b, afterIter); err != nil {
		return nil, err
	}
	return b.Finish(ctx)
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

func (o *Operator) Delete(ctx context.Context, s cadata.Store, x Root, key []byte) (*Root, error) {
	span := kvstreams.SingleItemSpan(key)
	return o.DeleteSpan(ctx, s, x, span)
}

func (o *Operator) DeleteSpan(ctx context.Context, s cadata.Store, x Root, span Span) (*Root, error) {
	b := o.makeBuilder(s)
	beforeIter := o.NewIterator(s, x, Span{End: span.Start})
	afterIter := o.NewIterator(s, x, Span{Start: span.End})
	if err := CopyAll(ctx, b, beforeIter); err != nil {
		return nil, err
	}
	if err := CopyAll(ctx, b, afterIter); err != nil {
		return nil, err
	}
	return b.Finish(ctx)
}

func (o *Operator) Filter(ctx context.Context, s cadata.Store, root Root, span Span, fn func(Entry) bool) (*Root, error) {
	b := o.makeBuilder(s)
	it := o.NewIterator(s, root, span)
	if err := kvstreams.ForEach(ctx, it, func(ent Entry) error {
		if fn(ent) {
			return b.Put(ctx, ent.Key, ent.Value)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return b.Finish(ctx)
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
