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

type Option func(op *Operator)

func WithDataOperator(ro gdat.Operator) Option {
	return func(o *Operator) {
		o.dop = ro
	}
}

// Operator holds common configuration for operations on gotkv instances.
// It has nothing to do with the state of a particular gotkv instance. It is NOT analagous to a collection object.
// It is safe for use by multiple goroutines.
type Operator struct {
	dop gdat.Operator
}

func NewOperator(opts ...Option) Operator {
	op := Operator{
		dop: gdat.NewOperator(),
	}
	for _, opt := range opts {
		opt(&op)
	}
	return op
}

func (o *Operator) Put(ctx context.Context, s cadata.Store, x Root, key, value []byte) (*Root, error) {
	return ptree.Mutate(ctx, s, &o.dop, x, ptree.Mutation{
		Span: ptree.SingleItemSpan(key),
		Fn:   func(*Entry) []Entry { return []Entry{{Key: key, Value: value}} },
	})
}

func (o *Operator) GetF(ctx context.Context, s cadata.Store, x Root, key []byte, fn func([]byte) error) error {
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
	span := ptree.SingleItemSpan(key)
	return o.DeleteSpan(ctx, s, x, span)
}

func (o *Operator) DeleteSpan(ctx context.Context, s cadata.Store, x Root, span Span) (*Root, error) {
	return ptree.Mutate(ctx, s, &o.dop, x, ptree.Mutation{
		Span: span,
		Fn:   func(*Entry) []Entry { return nil },
	})
}

func (o *Operator) Filter(ctx context.Context, s cadata.Store, root Root, span Span, fn func(Entry) bool) (*Root, error) {
	return ptree.Mutate(ctx, s, &o.dop, root, ptree.Mutation{
		Span: span,
		Fn: func(e *Entry) []Entry {
			if e == nil {
				return nil
			}
			if !fn(*e) {
				return nil
			}
			return []Entry{*e}
		},
	})
}

func (o *Operator) NewEmpty(ctx context.Context, s cadata.Store) (*Root, error) {
	b := o.NewBuilder(s)
	return b.Finish(ctx)
}

func (o *Operator) MaxKey(ctx context.Context, s cadata.Store, x Root, under []byte) ([]byte, error) {
	return ptree.MaxKey(ctx, s, x, under)
}

func (o *Operator) AddPrefix(ctx context.Context, s cadata.Store, x Root, prefix []byte) (*Root, error) {
	return ptree.AddPrefix(ctx, s, x, prefix)
}

func (o *Operator) RemovePrefix(ctx context.Context, s cadata.Store, x Root, prefix []byte) (*Root, error) {
	return ptree.RemovePrefix(ctx, s, x, prefix)
}

func (o *Operator) NewBuilder(s Store) Builder {
	return ptree.NewBuilder(s, &o.dop)
}

func (o *Operator) NewIterator(s Store, root Root, span Span) Iterator {
	return ptree.NewIterator(s, &o.dop, root, span)
}

func (o *Operator) ForEach(ctx context.Context, s Store, root Root, span Span, fn func(Entry) error) error {
	it := o.NewIterator(s, root, span)
	for {
		ent, err := it.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := fn(*ent); err != nil {
			return err
		}
	}
	return nil
}

func (o *Operator) Diff(ctx context.Context, s cadata.Store, left, right Root, span Span, fn ptree.DiffFn) error {
	return ptree.Diff(ctx, s, left, right, span, fn)
}
