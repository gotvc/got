package gotkv

import (
	"bytes"
	"context"
	"fmt"

	"github.com/brendoncarroll/go-exp/maybe"
	"github.com/brendoncarroll/go-exp/streams"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/gotvc/got/pkg/gotkv/ptree"
)

// Builder is used to construct GotKV instances
// by adding keys in lexicographical order.
type Builder struct {
	b ptree.Builder[Entry, Ref]
}

func (b *Builder) Put(ctx context.Context, key, value []byte) error {
	return b.b.Put(ctx, Entry{Key: key, Value: value})
}

func (b *Builder) Finish(ctx context.Context) (*Root, error) {
	root, err := b.b.Finish(ctx)
	return newRoot(root), err
}

// Iterator is used to iterate through entries in GotKV instances.
type Iterator struct {
	it ptree.Iterator[Entry, Ref]
}

func (it *Iterator) Next(ctx context.Context, dst *Entry) error {
	return it.it.Next(ctx, dst)
}

func (it *Iterator) Peek(ctx context.Context, dst *Entry) error {
	return it.it.Peek(ctx, dst)
}

func (it *Iterator) Seek(ctx context.Context, gteq []byte) error {
	return it.it.Seek(ctx, Entry{Key: gteq})
}

// Option is used to configure an Agent
type Option func(ag *Agent)

func WithDataAgent(ro *gdat.Agent) Option {
	return func(a *Agent) {
		a.da = ro
	}
}

// WithSeed returns an Option which sets the seed for an Agent.
// Seed affects node boundaries.
func WithSeed(seed *[16]byte) Option {
	if seed == nil {
		panic("seed cannot be nil")
	}
	return func(a *Agent) {
		a.seed = seed
	}
}

// Agent holds common configuration for operations on gotkv instances.
// It has nothing to do with the state of a particular gotkv instance. It is NOT analagous to a collection object.
// It is safe for use by multiple goroutines.
type Agent struct {
	da                *gdat.Agent
	maxSize, meanSize int
	seed              *[16]byte
}

// NewAgent returns an operator which will create nodes with mean size `meanSize`
// and maximum size `maxSize`.
func NewAgent(meanSize, maxSize int, opts ...Option) Agent {
	ag := Agent{
		da:       gdat.NewAgent(),
		meanSize: meanSize,
		maxSize:  maxSize,
	}
	if ag.meanSize <= 0 {
		panic(fmt.Sprintf("gotkv.NewAgent: invalid average size %d", ag.meanSize))
	}
	if ag.maxSize <= 0 {
		panic(fmt.Sprintf("gotkv.NewAgent: invalid max size %d", ag.maxSize))
	}
	for _, opt := range opts {
		opt(&ag)
	}
	return ag
}

func (a *Agent) MeanSize() int {
	return a.meanSize
}

func (a *Agent) MaxSize() int {
	return a.maxSize
}

// GetF calls fn with the value corresponding to key in the instance x.
// The value must not be used outside the callback.
func (a *Agent) GetF(ctx context.Context, s cadata.Getter, x Root, key []byte, fn func([]byte) error) error {
	it := a.NewIterator(s, x, kvstreams.SingleItemSpan(key))
	var ent Entry
	err := it.Next(ctx, &ent)
	if err != nil {
		if streams.IsEOS(err) {
			err = ErrKeyNotFound
		}
		return err
	}
	return fn(ent.Value)
}

// Get returns the value corresponding to key in the instance x.
func (a *Agent) Get(ctx context.Context, s cadata.Getter, x Root, key []byte) ([]byte, error) {
	var ret []byte
	if err := a.GetF(ctx, s, x, key, func(data []byte) error {
		ret = append([]byte{}, data...)
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

// Put returns a new version of the instance x with the entry at key corresponding to value.
// If an entry at key already exists it is overwritten, otherwise it will be created.
func (a *Agent) Put(ctx context.Context, s cadata.Store, x Root, key, value []byte) (*Root, error) {
	return a.Mutate(ctx, s, x, Mutation{
		Span:    SingleKeySpan(key),
		Entries: []Entry{{Key: key, Value: value}},
	})
}

// Delete returns a new version of the instance x where there is no entry for key.
// If key does not exist no error is returned.
func (a *Agent) Delete(ctx context.Context, s cadata.Store, x Root, key []byte) (*Root, error) {
	return a.DeleteSpan(ctx, s, x, kvstreams.SingleItemSpan(key))
}

// DeleteSpan returns a new version of the instance x where there are no entries contained in span.
func (a *Agent) DeleteSpan(ctx context.Context, s cadata.Store, x Root, span Span) (*Root, error) {
	return a.Mutate(ctx, s, x, Mutation{
		Span: span,
	})
}

// NewEmpty returns a new GotKV instance with no entries.
func (a *Agent) NewEmpty(ctx context.Context, s cadata.Store) (*Root, error) {
	b := a.NewBuilder(s)
	return b.Finish(ctx)
}

// MaxEntry returns the entry in the instance x, within span, with the greatest lexicographic value.
func (a *Agent) MaxEntry(ctx context.Context, s cadata.Getter, x Root, span Span) (*Entry, error) {
	rp := ptree.ReadParams[Entry, Ref]{
		Store:           &ptreeGetter{ag: a.da, s: s},
		Compare:         compareEntries,
		NewIndexDecoder: newIndexDecoder,
		NewDecoder:      newDecoder,
	}
	var dst Entry
	if err := ptree.MaxEntry(ctx, rp, x.toPtree(), maybe.Just(Entry{Key: span.End}), &dst); err != nil {
		if streams.IsEOS(err) {
			return nil, nil
		}
		return nil, err
	}
	return &dst, nil
}

func (a *Agent) HasPrefix(ctx context.Context, s cadata.Getter, x Root, prefix []byte) (bool, error) {
	if !bytes.HasPrefix(x.First, prefix) {
		return false, nil
	}
	maxEnt, err := a.MaxEntry(ctx, s, x, kvstreams.TotalSpan())
	if err != nil {
		return false, err
	}
	if !bytes.HasPrefix(maxEnt.Key, prefix) {
		return false, nil
	}
	return true, nil
}

// AddPrefix prepends prefix to all the keys in instance x.
// This is a O(1) operation.
func (a *Agent) AddPrefix(x Root, prefix []byte) Root {
	return AddPrefix(x, prefix)
}

// RemovePrefix removes a prefix from all the keys in instance x.
// RemotePrefix errors if all the entries in x do not share a common prefix.
// This is a O(1) operation.
func (a *Agent) RemovePrefix(ctx context.Context, s cadata.Getter, x Root, prefix []byte) (*Root, error) {
	if yes, err := a.HasPrefix(ctx, s, x, prefix); err != nil {
		return nil, err
	} else if yes {
		return nil, fmt.Errorf("tree does not have prefix %q", prefix)
	}
	y := Root{
		First: append([]byte{}, x.First[len(prefix):]...),
		Ref:   x.Ref,
		Depth: x.Depth,
	}
	return &y, nil
}

// NewBuilder returns a Builder for constructing a GotKV instance.
// Data will be persisted to s.
func (a *Agent) NewBuilder(s Store) *Builder {
	b := ptree.NewBuilder(ptree.BuilderParams[Entry, Ref]{
		Store:           &ptreeStore{ag: a.da, s: s},
		MeanSize:        a.meanSize,
		MaxSize:         a.maxSize,
		Seed:            a.seed,
		NewEncoder:      func() ptree.Encoder[Entry] { return &Encoder{} },
		NewIndexEncoder: func() ptree.IndexEncoder[Entry, Ref] { return &IndexEncoder{} },
		Compare:         compareEntries,
		Copy:            copyEntry,
	})
	return &Builder{b: *b}
}

// NewIterator returns an iterator for the instance rooted at x, which
// will emit all keys within span in the instance.
func (a *Agent) NewIterator(s Getter, root Root, span Span) *Iterator {
	if span.End != nil && bytes.Compare(span.Begin, span.End) > 0 {
		panic(fmt.Sprintf("cannot iterate over descending span. begin=%q end=%q", span.Begin, span.End))
	}
	it := ptree.NewIterator(ptree.IteratorParams[Entry, Ref]{
		Store:           &ptreeGetter{ag: a.da, s: s},
		NewDecoder:      newDecoder,
		NewIndexDecoder: newIndexDecoder,
		Compare:         compareEntries,
		Copy:            copyEntry,

		Root: root.toPtree(),
		Span: convertSpan(span),
	})
	return &Iterator{it: *it}
}

// ForEach calls fn with every entry, in the GotKV instance rooted at root, contained in span, in lexicographical order.
// If fn returns an error, ForEach immediately returns that error.
func (a *Agent) ForEach(ctx context.Context, s Getter, root Root, span Span, fn func(Entry) error) error {
	it := a.NewIterator(s, root, span)
	var ent Entry
	for {
		if err := it.Next(ctx, &ent); err != nil {
			if streams.IsEOS(err) {
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
// The result of applying a Mutation is that the entire contents of the Span are replaced with Entries.
type Mutation struct {
	Span    Span
	Entries []Entry
}

// Mutate applies a batch of mutations to the tree x.
func (a *Agent) Mutate(ctx context.Context, s cadata.Store, x Root, mutations ...Mutation) (*Root, error) {
	iters := make([]kvstreams.Iterator, 2*len(mutations)+1)
	var begin []byte
	for i, mut := range mutations {
		if err := checkMutation(mut); err != nil {
			return nil, err
		}
		if i > 0 {
			if bytes.Compare(mut.Span.Begin, mutations[i-1].Span.End) < 0 {
				return nil, fmt.Errorf("spans out of order %d start: %q < %d end: %q", i, mut.Span.Begin, i-1, mut.Span.End)
			}
		}
		beforeIter := a.NewIterator(s, x, Span{
			Begin: begin,
			End:   append([]byte{}, mut.Span.Begin...), // ensure this isn't nil, there must be an upper bound.
		})
		iters[2*i] = beforeIter
		iters[2*i+1] = kvstreams.NewLiteral(mut.Entries)
		begin = mut.Span.End
	}
	iters[len(iters)-1] = a.NewIterator(s, x, Span{
		Begin: begin,
		End:   nil,
	})
	return a.Concat(ctx, s, iters...)
}

func checkMutation(mut Mutation) error {
	for _, ent := range mut.Entries {
		if !mut.Span.Contains(ent.Key) {
			return fmt.Errorf("mutation span %v does not contain entry key %q", mut.Span, ent.Key)
		}
	}
	return nil
}

// Concat copies data from the iterators in order.
// If the iterators produce out of order keys concat errors.
func (a *Agent) Concat(ctx context.Context, s cadata.Store, iters ...kvstreams.Iterator) (*Root, error) {
	b := a.NewBuilder(s)
	for _, iter := range iters {
		if err := CopyAll(ctx, b, iter); err != nil {
			return nil, err
		}
	}
	return b.Finish(ctx)
}
