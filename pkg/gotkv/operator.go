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

// Builder is used to construct GotKV instances
// by adding keys in lexicographical order.
type Builder = ptree.Builder

// Iterator is used to iterate through entries in GotKV instances.
type Iterator = ptree.Iterator

// Option is used to configure an Operator
type Option func(op *Operator)

func WithDataOperator(ro gdat.Operator) Option {
	return func(o *Operator) {
		o.dop = ro
	}
}

// WithSeed returns an Option which sets the seed for an Operator.
// Seed affects node boundaries.
func WithSeed(seed *[16]byte) Option {
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
	dop               gdat.Operator
	maxSize, meanSize int
	seed              *[16]byte
}

// NewOperator returns an operator which will create nodes with mean size `meanSize`
// and maximum size `maxSize`.
func NewOperator(meanSize, maxSize int, opts ...Option) Operator {
	op := Operator{
		dop:      gdat.NewOperator(),
		meanSize: meanSize,
		maxSize:  maxSize,
	}
	if op.meanSize <= 0 {
		panic(fmt.Sprintf("gotkv.NewOperator: invalid average size %d", op.meanSize))
	}
	if op.maxSize <= 0 {
		panic(fmt.Sprintf("gotkv.NewOperator: invalid max size %d", op.maxSize))
	}
	for _, opt := range opts {
		opt(&op)
	}
	return op
}

func (o *Operator) MeanSize() int {
	return o.meanSize
}

func (o *Operator) MaxSize() int {
	return o.maxSize
}

// GetF calls fn with the value corresponding to key in the instance x.
// The value must not be used outside the callback.
func (o *Operator) GetF(ctx context.Context, s cadata.Getter, x Root, key []byte, fn func([]byte) error) error {
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

// Get returns the value corresponding to key in the instance x.
func (o *Operator) Get(ctx context.Context, s cadata.Getter, x Root, key []byte) ([]byte, error) {
	var ret []byte
	if err := o.GetF(ctx, s, x, key, func(data []byte) error {
		ret = append([]byte{}, data...)
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

// Put returns a new version of the instance x with the entry at key corresponding to value.
// If an entry at key already exists it is overwritten, otherwise it will be created.
func (o *Operator) Put(ctx context.Context, s cadata.Store, x Root, key, value []byte) (*Root, error) {
	return o.Mutate(ctx, s, x, Mutation{
		Span:    SingleKeySpan(key),
		Entries: []Entry{{Key: key, Value: value}},
	})
}

// Delete returns a new version of the instance x where there is no entry for key.
// If key does not exist no error is returned.
func (o *Operator) Delete(ctx context.Context, s cadata.Store, x Root, key []byte) (*Root, error) {
	return o.DeleteSpan(ctx, s, x, kvstreams.SingleItemSpan(key))
}

// DeleteSpan returns a new version of the instance x where there are no entries contained in span.
func (o *Operator) DeleteSpan(ctx context.Context, s cadata.Store, x Root, span Span) (*Root, error) {
	return o.Mutate(ctx, s, x, Mutation{
		Span: span,
	})
}

// NewEmpty returns a new GotKV instance with no entries.
func (o *Operator) NewEmpty(ctx context.Context, s cadata.Store) (*Root, error) {
	b := o.NewBuilder(s)
	return b.Finish(ctx)
}

// MaxEntry returns the entry in the instance x, within span, with the greatest lexicographic value.
func (o *Operator) MaxEntry(ctx context.Context, s cadata.Getter, x Root, span Span) (*Entry, error) {
	ps := &ptreeGetter{op: &o.dop, s: s}
	return ptree.MaxEntry(ctx, bytes.Compare, ps, x, span)
}

// AddPrefix prepends prefix to all the keys in instance x.
// This is a O(1) operation.
func (o *Operator) AddPrefix(x Root, prefix []byte) Root {
	return ptree.AddPrefix(x, prefix)
}

// RemovePrefix removes a prefix from all the keys in instance x.
// RemotePrefix errors if all the entries in x do not share a common prefix.
// This is a O(1) operation.
func (o *Operator) RemovePrefix(ctx context.Context, s cadata.Getter, x Root, prefix []byte) (*Root, error) {
	ps := &ptreeGetter{op: &o.dop, s: s}
	return ptree.RemovePrefix(ctx, bytes.Compare, ps, x, prefix)
}

// NewBuilder returns a Builder for constructing a GotKV instance.
// Data will be persisted to s.
func (o *Operator) NewBuilder(s Store) *Builder {
	return o.makeBuilder(s)
}

// NewIterator returns an iterator for the instance rooted at x, which
// will emit all keys within span in the instance.
func (o *Operator) NewIterator(s Getter, root Root, span Span) *Iterator {
	return ptree.NewIterator(ptree.IteratorParams{
		Store:   &ptreeGetter{op: &o.dop, s: s},
		Compare: bytes.Compare,
		Root:    root,
		Span:    span,
	})
}

func (o *Operator) makeBuilder(s cadata.Store) *ptree.Builder {
	return ptree.NewBuilder(ptree.BuilderParams{
		Store:    &ptreeStore{op: &o.dop, s: s},
		MeanSize: o.meanSize,
		MaxSize:  o.maxSize,
		Seed:     o.seed,
		Compare:  bytes.Compare,
	})
}

// ForEach calls fn with every entry, in the GotKV instance rooted at root, contained in span, in lexicographical order.
// If fn returns an error, ForEach immediately returns that error.
func (o *Operator) ForEach(ctx context.Context, s Getter, root Root, span Span, fn func(Entry) error) error {
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

// Mutation represents a declarative change to a Span of entries.
// The result of applying mutation is that
type Mutation struct {
	Span    Span
	Entries []Entry
}

// Mutate applies a batch of mutations to the tree x.
func (o *Operator) Mutate(ctx context.Context, s cadata.Store, x Root, mutations ...Mutation) (*Root, error) {
	iters := make([]kvstreams.Iterator, 2*len(mutations)+1)
	var begin []byte
	for i, mut := range mutations {
		if err := checkMutation(mut); err != nil {
			return nil, err
		}
		if i > 0 {
			if bytes.Compare(mut.Span.Begin, mutations[i-1].Span.End) < 0 {
				return nil, errors.Errorf("spans out of order %d start: %q < %d end: %q", i, mut.Span.Begin, i-1, mut.Span.End)
			}
		}
		beforeIter := o.NewIterator(s, x, Span{
			Begin: begin,
			End:   append([]byte{}, mut.Span.Begin...), // ensure this isn't nil, there must be an upper bound.
		})
		iters[2*i] = beforeIter
		iters[2*i+1] = kvstreams.NewLiteral(mut.Entries)
		begin = mut.Span.End
	}
	iters[len(iters)-1] = o.NewIterator(s, x, Span{
		Begin: begin,
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
