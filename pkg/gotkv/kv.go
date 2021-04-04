package gotkv

import (
	"context"
	"io"

	"github.com/blobcache/blobcache/pkg/bccrypto"
	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/ptree"
	"github.com/brendoncarroll/got/pkg/refs"
	"github.com/pkg/errors"
)

type (
	Store = cadata.Store
	ID    = cadata.ID
	Ref   = refs.Ref

	Entry = ptree.Entry
	Root  = ptree.Root
	Span  = ptree.Span
)

var ErrKeyNotFound = errors.Errorf("key not found")

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
	NewEmpty(ctx context.Context, s Store) (*Root, error)

	Get(ctx context.Context, s Store, x Root, key []byte) ([]byte, error)
	GetF(ctx context.Context, s Store, x Root, key []byte, fn func([]byte) error) error

	Put(ctx context.Context, s Store, x Root, key, value []byte) (*Root, error)
	Delete(ctx context.Context, s Store, x Root, key []byte) (*Root, error)
	DeleteSpan(ctx context.Context, s Store, x Root, span Span) (*Root, error)

	MaxKey(ctx context.Context, s Store, x Root, under []byte) ([]byte, error)

	Reduce(ctx context.Context, s Store, xs []Root, fn ReduceFunc) (*Root, error)

	// AddPrefix(ctx context.Context, s Store, x Root, prefix []byte) (*Root, error)

	NewBuilder(s Store) Builder
	NewIterator(s Store, x Root, span Span) Iterator
}

type Option func(op *operator)

func WithEncryptionKeyFunc(kf bccrypto.KeyFunc) Option {
	return func(op *operator) {
		op.keyFunc = kf
	}
}

type operator struct {
	keyFunc bccrypto.KeyFunc
	// refs.Cache
}

func NewOperator(opts ...Option) Operator {
	op := &operator{}
	for _, opt := range opts {
		opt(op)
	}
	return op
}

func (o *operator) Put(ctx context.Context, s cadata.Store, x Root, key, value []byte) (*Root, error) {
	return ptree.Mutate(ctx, s, x, ptree.Mutation{
		Span: ptree.SingleItemSpan(key),
		Fn:   func(*Entry) []Entry { return []Entry{{Key: key, Value: value}} },
	})
}

func (o *operator) GetF(ctx context.Context, s cadata.Store, x Root, key []byte, fn func([]byte) error) error {
	it := o.NewIterator(s, x, ptree.SingleItemSpan(key))
	ent, err := it.Next(ctx)
	if err == io.EOF {
		return ErrKeyNotFound
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
	return ptree.Mutate(ctx, s, x, ptree.Mutation{
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

func (o *operator) NewBuilder(s Store) Builder {
	return ptree.NewBuilder(s)
}

func (o *operator) NewIterator(s Store, root Root, span Span) Iterator {
	return ptree.NewIterator(s, root, span)
}
