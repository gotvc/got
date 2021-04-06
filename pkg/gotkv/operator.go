package gotkv

import (
	"context"
	"io"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/ptree"
)

type Builder interface {
	Put(ctx context.Context, key, value []byte) error
	Finish(ctx context.Context) (*Root, error)
}

type Iterator interface {
	Next(ctx context.Context) (*Entry, error)
	Seek(ctx context.Context, key []byte) error
}

// Operator holds common configuration for operations on gotkv instances.
// It has nothing to do with the state of a particular gotkv instance. It is NOT analagous to a collection object.
// It is safe for use by multiple goroutines.
type Operator interface {
	// NewEmpty creates a new, empty instance and returns the root
	NewEmpty(ctx context.Context, s Store) (*Root, error)

	Get(ctx context.Context, s Store, x Root, key []byte) ([]byte, error)
	GetF(ctx context.Context, s Store, x Root, key []byte, fn func([]byte) error) error

	Put(ctx context.Context, s Store, x Root, key, value []byte) (*Root, error)
	Delete(ctx context.Context, s Store, x Root, key []byte) (*Root, error)
	DeleteSpan(ctx context.Context, s Store, x Root, span Span) (*Root, error)

	MaxKey(ctx context.Context, s Store, x Root, under []byte) ([]byte, error)

	Reduce(ctx context.Context, s Store, xs []Root, fn ReduceFunc) (*Root, error)

	// PrependKeys adds prefix in front of all the keys.
	PrependKeys(ctx context.Context, s Store, x Root, prefix []byte) (*Root, error)

	NewBuilder(s Store) Builder
	NewIterator(s Store, x Root, span Span) Iterator
}

type Option func(op *operator)

func WithRefOperator(ro *gdat.Operator) Option {
	return func(o *operator) {
		o.dop = ro
	}
}

type operator struct {
	dop *gdat.Operator
}

func NewOperator(opts ...Option) Operator {
	op := &operator{
		dop: gdat.NewOperator(),
	}
	for _, opt := range opts {
		opt(op)
	}
	return op
}

func (o *operator) Put(ctx context.Context, s cadata.Store, x Root, key, value []byte) (*Root, error) {
	return ptree.Mutate(ctx, s, o.dop, x, ptree.Mutation{
		Span: ptree.SingleItemSpan(key),
		Fn:   func(*Entry) []Entry { return []Entry{{Key: key, Value: value}} },
	})
}

func (o *operator) GetF(ctx context.Context, s cadata.Store, x Root, key []byte, fn func([]byte) error) error {
	it := o.NewIterator(s, x, ptree.SingleItemSpan(key))
	ent, err := it.Next(ctx)
	if err != nil {
		if err == io.EOF {
			err = ErrKeyNotFound
		}
		return err
	}
	return fn(ent.Value)
}

func (o *operator) Get(ctx context.Context, s cadata.Store, x Root, key []byte) ([]byte, error) {
	var ret []byte
	if err := o.GetF(ctx, s, x, key, func(data []byte) error {
		ret = append([]byte{}, data...)
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (o *operator) Delete(ctx context.Context, s cadata.Store, x Root, key []byte) (*Root, error) {
	span := ptree.SingleItemSpan(key)
	return o.DeleteSpan(ctx, s, x, span)
}

func (o *operator) DeleteSpan(ctx context.Context, s cadata.Store, x Root, span Span) (*Root, error) {
	return ptree.Mutate(ctx, s, o.dop, x, ptree.Mutation{
		Span: span,
		Fn:   func(*Entry) []Entry { return nil },
	})
}

func (o *operator) NewEmpty(ctx context.Context, s cadata.Store) (*Root, error) {
	b := o.NewBuilder(s)
	return b.Finish(ctx)
}

func (o *operator) MaxKey(ctx context.Context, s cadata.Store, x Root, under []byte) ([]byte, error) {
	return ptree.MaxKey(ctx, s, x, under)
}

func (o *operator) PrependKeys(ctx context.Context, s cadata.Store, x Root, prefix []byte) (*Root, error) {
	return ptree.Mutate(ctx, s, o.dop, x, ptree.Mutation{
		Span: ptree.TotalSpan(),
		Fn: func(ent *Entry) []Entry {
			if ent == nil {
				return nil
			}
			ent2 := Entry{
				Key:   append(prefix, ent.Value...),
				Value: ent.Value,
			}
			return []Entry{ent2}
		},
	})
}

func (o *operator) NewBuilder(s Store) Builder {
	return ptree.NewBuilder(s, o.dop)
}

func (o *operator) NewIterator(s Store, root Root, span Span) Iterator {
	return ptree.NewIterator(s, root, span)
}
