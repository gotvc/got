package ptree

import (
	"context"
	"fmt"

	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state"
)

var _ streams.Iterator[int] = &Iterator[int, int]{}

type Iterator[T, Ref any] struct {
	p IteratorParams[T, Ref]

	levels   []iterLevel[T, Ref]
	span     state.Span[T]
	readRoot bool
}

type iterLevel[T, Ref any] struct {
	entries *StreamReader[T, Ref]
	indexes *StreamReader[Index[T, Ref], Ref]
}

func (il iterLevel[T, Ref]) IsZero() bool {
	return il == (iterLevel[T, Ref]{})
}

func (il iterLevel[T, Ref]) Buffered() int {
	if il.entries != nil {
		return il.entries.Buffered()
	}
	if il.indexes != nil {
		return il.indexes.Buffered()
	}
	panic("empty iterLevel")
}

func (il iterLevel[T, Ref]) Seek(ctx context.Context, gteq T) error {
	if il.entries != nil {
		return il.entries.Seek(ctx, gteq)
	}
	if il.indexes != nil {
		return il.indexes.Seek(ctx, Index[T, Ref]{
			Span: state.TotalSpan[T]().WithLowerIncl(gteq),
		})
	}
	return nil
}

type IteratorParams[T, Ref any] struct {
	Store           Getter[Ref]
	NewDecoder      func() Decoder[T, Ref]
	NewIndexDecoder func() IndexDecoder[T, Ref]
	Compare         CompareFunc[T]
	Copy            func(dst *T, src T)

	Root Root[T, Ref]
	Span state.Span[T]
}

func NewIterator[T, Ref any](params IteratorParams[T, Ref]) *Iterator[T, Ref] {
	return &Iterator[T, Ref]{
		p: params,

		levels: make([]iterLevel[T, Ref], params.Root.Depth+1),
		span:   cloneSpan(params.Span, params.Copy),
	}
}

func (it *Iterator[T, Ref]) Next(ctx context.Context, dst []T) (int, error) {
	if err := it.next(ctx, 0, dual[T, Ref]{Entry: &dst[0]}); err != nil {
		return 0, err
	}
	return 1, nil
}

func (it *Iterator[T, Ref]) Peek(ctx context.Context, dst *T) error {
	return it.peek(ctx, 0, dual[T, Ref]{Entry: dst})
}

func (it *Iterator[T, Ref]) Seek(ctx context.Context, gteq T) error {
	// NOTE: this function is called by it.next.  We are not allowed to call it.next in this function.
	if it.span.Compare(gteq, it.p.Compare) > 0 {
		lb, _ := it.span.LowerBound()
		panic(fmt.Errorf("cannot seek backwards: last=%v gteq=%v", lb, gteq))
	}
	it.setGteq(gteq)
	for i := len(it.levels) - 1; i >= 0; i-- {
		il, err := it.getLevel(ctx, i)
		if err != nil {
			return err
		}
		if err := il.Seek(ctx, gteq); err != nil {
			return err
		}
	}
	return nil
}

func (it *Iterator[T, Ref]) next(ctx context.Context, level int, dst dual[T, Ref]) error {
	if sl, err := it.syncLevel(); err != nil {
		return err
	} else if sl < level {
		return fmt.Errorf("cannot read from level %d, only synced to %d", level, sl)
	}
	il, err := it.getLevel(ctx, level)
	if err != nil {
		return err
	}
	if level == 0 {
		if err := il.entries.Next(ctx, dst.Entry); err != nil {
			return err
		}
		if err := it.checkEntry(*dst.Entry); err != nil {
			return err
		}
		it.setGt(*dst.Entry)
		return nil
	} else {
		if err := il.indexes.Next(ctx, dst.Index); err != nil {
			return err
		}
		if ub, ok := dst.Index.Span.UpperBound(); ok {
			if dst.Index.Span.IncludesUpper() {
				it.setGt(ub)
			} else {
				it.setGteq(ub)
			}
		}
		return nil
	}
}

func (it *Iterator[T, Ref]) peek(ctx context.Context, level int, dst dual[T, Ref]) error {
	if sl, err := it.syncLevel(); err != nil {
		return err
	} else if sl < level {
		return fmt.Errorf("cannot read from level %d, only synced to %d", level, sl)
	}
	il, err := it.getLevel(ctx, level)
	if err != nil {
		return err
	}
	if level == 0 {
		if err := il.entries.Peek(ctx, dst.Entry); err != nil {
			return err
		}
		if err := it.checkEntry(*dst.Entry); err != nil {
			return err
		}
		return nil
	} else {
		return il.indexes.Peek(ctx, dst.Index)
	}
}

func (it *Iterator[T, Ref]) getLevel(ctx context.Context, level int) (iterLevel[T, Ref], error) {
	if level >= len(it.levels) {
		panic(level)
	}
	if it.levels[level].IsZero() {
		il, err := it.makeLevel(ctx, level)
		if err != nil {
			return iterLevel[T, Ref]{}, err
		}
		it.levels[level] = il
	}
	return it.levels[level], nil
}

func (it *Iterator[T, Ref]) makeLevel(ctx context.Context, level int) (ret iterLevel[T, Ref], _ error) {
	switch {
	case level == 0:
		sr := NewStreamReader(StreamReaderParams[T, Ref]{
			Store:   it.p.Store,
			Decoder: it.p.NewDecoder(),
			Compare: it.p.Compare,
			NextIndex: func(ctx context.Context, dst *Index[T, Ref]) error {
				if level == len(it.levels)-1 {
					if it.readRoot {
						return streams.EOS()
					} else {
						*dst = it.p.Root.Index
						it.readRoot = true
						return nil
					}
				} else {
					il, err := it.getLevel(ctx, level+1)
					if err != nil {
						return err
					}
					return il.indexes.Next(ctx, dst)
				}
			},
		})
		ret = iterLevel[T, Ref]{entries: sr}
	case level < len(it.levels):
		sr := NewStreamReader(StreamReaderParams[Index[T, Ref], Ref]{
			Store:   it.p.Store,
			Decoder: metaDecoder[T, Ref]{it.p.NewIndexDecoder()},
			Compare: upgradeCompare[T, Ref](it.p.Compare),
			NextIndex: func(ctx context.Context, dst *Index[Index[T, Ref], Ref]) error {
				if level == len(it.levels)-1 {
					if it.readRoot {
						return streams.EOS()
					}
					*dst = metaIndex(it.p.Root.Index)
					it.readRoot = true
					return nil
				} else {
					il, err := it.getLevel(ctx, level+1)
					if err != nil {
						return err
					}
					var idx Index[T, Ref]
					if err := il.indexes.Next(ctx, &idx); err != nil {
						return err
					}
					r := indexToRoot(idx, uint8(level+1))
					*dst = metaIndex(r.Index)
					return nil
				}
			},
		})
		ret = iterLevel[T, Ref]{indexes: sr}
	default:
		panic(level)
	}
	if lb, ok := it.span.LowerBound(); ok {
		if !it.span.IncludesLower() {
			panic("exclusive lower bounds not supported")
		}
		if err := ret.Seek(ctx, lb); err != nil {
			return iterLevel[T, Ref]{}, err
		}
	}
	return ret, nil
}

func (it *Iterator[T, Ref]) syncLevel() (int, error) {
	if it.levels[len(it.levels)-1].IsZero() {
		return 0, nil
	}
	// bot is the index below which all levels are synced
	//
	// We can only copy when all the below levels are synced
	var bot int
	for i := 1; i < len(it.levels); i++ {
		// check that previous level has nothing buffered.
		if it.levels[i-1].Buffered() > 0 {
			break
		}
		if i > 0 {
			var idx Index[T, Ref]
			if err := it.levels[i].indexes.PeekNoLoad(&idx); err != nil {
				if streams.IsEOS(err) {
					continue // we can copy if it's okay to copy from the next one.
				}
				return 0, err
			} else {
				// if the index is not natural, then we can't copy it, we have to copy what it points to.
				if !idx.IsNatural {
					break
				}
				// if there is an index here and it is natural, then we can definitely copy from the previous level
				bot = i - 1
				// if the index points to things beyond the iterators span, then we cannot copy it.
				if ub, ok := it.span.UpperBound(); ok {
					// The indexes' span is entirely after OR includes ub.
					if idx.Span.Compare(ub, it.p.Compare) >= 0 {
						break
					}
				}
			}
		}
		bot = i
	}
	return bot, nil
}

func (it *Iterator[T, Ref]) checkEntry(x T) error {
	if cmp := it.span.Compare(x, it.p.Compare); cmp < 0 {
		return streams.EOS()
	} else if cmp > 0 {
		panic(fmt.Sprintf("entry below lower bound span=%v entry=%v", it.span, x))
	} else {
		return nil
	}
}

func (it *Iterator[T, Ref]) setGteq(x T) {
	var x2 T
	it.p.Copy(&x2, x)
	it.span = it.span.WithLowerIncl(x2)
}

func (it *Iterator[T, Ref]) setGt(x T) {
	var x2 T
	it.p.Copy(&x2, x)
	it.span = it.span.WithLowerExcl(x2)
}

type metaDecoder[T, Ref any] struct {
	inner IndexDecoder[T, Ref]
}

func (d metaDecoder[T, Ref]) Read(src []byte, dst *Index[T, Ref]) (int, error) {
	return d.inner.Read(src, dst)
}

func (d metaDecoder[T, Ref]) Peek(src []byte, dst *Index[T, Ref]) error {
	return d.inner.Peek(src, dst)
}

func (d metaDecoder[T, Ref]) Reset(parent Index[Index[T, Ref], Ref]) {
	d.inner.Reset(flattenIndex(parent))
}
