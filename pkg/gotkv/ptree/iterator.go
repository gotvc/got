package ptree

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/brendoncarroll/go-state"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

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
			First: Just(gteq),
		})
	}
	return nil
}

type IteratorParams[T, Ref any] struct {
	Store           Getter[Ref]
	NewDecoder      func() Decoder[T, Ref]
	NewIndexDecoder func() Decoder[Index[T, Ref], Ref]
	Compare         CompareFunc[T]
	Copy            func(dst *T, src T)

	Root Root[T, Ref]
	Span state.Span[T]
}

func NewIterator[T, Ref any](params IteratorParams[T, Ref]) *Iterator[T, Ref] {
	it := &Iterator[T, Ref]{
		p: params,

		levels: make([]iterLevel[T, Ref], params.Root.Depth+1),
		span:   params.Span,
	}
	return it
}

func (it *Iterator[T, Ref]) Next(ctx context.Context, dst *T) error {
	return it.next(ctx, 0, dual[T, Ref]{Entry: dst})
}

func (it *Iterator[T, Ref]) Peek(ctx context.Context, dst *T) error {
	return it.peek(ctx, 0, dual[T, Ref]{Entry: dst})
}

func (it *Iterator[T, Ref]) Seek(ctx context.Context, gteq T) error {
	if it.span.Compare(gteq, it.p.Compare) > 0 {
		return errors.New("cannot seek backwards")
	}
	for i := len(it.levels) - 1; i >= 0; i-- {
		if err := it.levels[i].Seek(ctx, gteq); err != nil {
			return err
		}
	}
	return nil
}

func (it *Iterator[T, Ref]) next(ctx context.Context, level int, dst dual[T, Ref]) error {
	if it.syncLevel() < level {
		return fmt.Errorf("cannot read from level %d, only synced to %d", level, it.syncLevel())
	}
	il, err := it.getLevel(ctx, level)
	if err != nil {
		return err
	}
	// TODO: update position
	if il.entries != nil {
		if err := il.entries.Next(ctx, dst.Entry); err != nil {
			return err
		}
		it.setGt(*dst.Entry)
		return nil
	}
	if il.indexes != nil {
		if err := il.indexes.Next(ctx, dst.Index); err != nil {
			return err
		}
		return nil
	}
	panic("iterLevel is empty")
}

func (it *Iterator[T, Ref]) peek(ctx context.Context, level int, dst dual[T, Ref]) error {
	if it.syncLevel() < level {
		return fmt.Errorf("cannot read from level %d, only synced to %d", level, it.syncLevel())
	}
	il, err := it.getLevel(ctx, level)
	if err != nil {
		return err
	}
	if il.entries != nil {
		return il.entries.Peek(ctx, dst.Entry)
	}
	if il.indexes != nil {
		return il.indexes.Peek(ctx, dst.Index)
	}
	panic("iterLevel is empty")
}

func (it *Iterator[T, Ref]) getLevel(ctx context.Context, level int) (iterLevel[T, Ref], error) {
	if level >= len(it.levels) {
		return iterLevel[T, Ref]{}, kvstreams.EOS
	}
	if it.levels[level].IsZero() {
		il, err := it.makeLevel(ctx, level)
		if err != nil {
			return iterLevel[T, Ref]{}, nil
		}
		it.levels[level] = il
	}
	return it.levels[level], nil
}

func (it *Iterator[T, Ref]) makeLevel(ctx context.Context, level int) (iterLevel[T, Ref], error) {
	switch {
	case level == 0:
		sr := NewStreamReader(StreamReaderParams[T, Ref]{
			Store:   it.p.Store,
			Decoder: it.p.NewDecoder(),
			Compare: it.p.Compare,
			NextIndex: func(ctx context.Context, dst *Index[T, Ref]) error {
				il, err := it.getLevel(ctx, level+1)
				if err != nil {
					return err
				}
				return il.indexes.Next(ctx, dst)
			},
		})
		return iterLevel[T, Ref]{entries: sr}, nil
	case level < len(it.levels)-1:
		sr := NewStreamReader(StreamReaderParams[Index[T, Ref], Ref]{
			Store:   it.p.Store,
			Decoder: it.p.NewIndexDecoder(),
			Compare: upgradeCompare[T, Ref](it.p.Compare),
			NextIndex: func(ctx context.Context, dst *Index[Index[T, Ref], Ref]) error {
				il, err := it.getLevel(ctx, level+1)
				if err != nil {
					return err
				}
				var idx Index[T, Ref]
				if err := il.indexes.Next(ctx, &idx); err != nil {
					return err
				}
				r := indexToRoot(idx, uint8(level+1))
				*dst = r.Index2()
				return nil
			},
		})
		return iterLevel[T, Ref]{indexes: sr}, nil
	default:
		sr := NewStreamReader(StreamReaderParams[Index[T, Ref], Ref]{
			Store:   it.p.Store,
			Decoder: it.p.NewIndexDecoder(),
			Compare: upgradeCompare[T, Ref](it.p.Compare),
			NextIndex: func(ctx context.Context, dst *Index[Index[T, Ref], Ref]) error {
				if it.readRoot {
					return EOS
				}
				*dst = it.p.Root.Index2()
				it.readRoot = true
				return nil
			},
		})
		return iterLevel[T, Ref]{indexes: sr}, nil
	}
}

func (it *Iterator[T, Ref]) syncLevel() int {
	if it.levels[len(it.levels)-1].IsZero() {
		return 0
	}
	// bot is the index below which all levels are synced
	//
	// We can only copy when all the below levels are synced
	var bot int
	for i := range it.levels {
		bot = i
		if it.levels[i].Buffered() > 0 {
			break
		}
	}
	// top is maximum index where the level has more than 1 entry
	// top is required because indexes at the right most side of the tree cannot be copied
	// since they could point to incomplete nodes.
	// the iterator's span causes us to consider some otherwise complete nodes incomplete.
	//
	// We can only copy a full node, with another node after it.
	var top = int(math.MaxInt)
	for i := len(it.levels) - 2; i >= 0; i-- {
		top = i
		if it.levels[i+1].Buffered() > 0 {
			break
		}
		// if len(it.levels[i]) > 1 && it.span.Contains(it.levels[i][1], it.compare) {
		// 	break
		// }
	}
	return min(bot, top)
}

// func (it *Iterator[T, Ref]) advanceLevel(level int, updatePos bool) {
// 	entries := it.levels[level]
// 	it.levels[level] = entries[1:]
// 	if !updatePos {
// 		return
// 	}
// 	for i := level; i < len(it.levels); i++ {
// 		il := it.levels[i]
// 		if len(il.entries) > 0 {
// 			it.setGt(il.entries[0])
// 			return
// 		} else if len(il.indexes) > 0 {
// 			// TODO
// 			//it.setGt(il.indexes[0])
// 			return
// 		}
// 	}
// 	// end of the stream
// }

func (it *Iterator[T, Ref]) setGteq(x T) {
	var x2 T
	it.p.Copy(&x2, x)
	it.span = it.span.WithLowerIncl(x2)
}

func (it *Iterator[T, Ref]) setGt(x T) {
	var x2 T
	it.p.Copy(&x2, x)
	it.span = it.span.WithLowerExcl(x)
}

// func filterEntries[T any](xs []T, span state.Span[T], cmp func(a, b T) int) []T {
// 	ret := xs[:0]
// 	for i := range xs {
// 		if span.Contains(xs[i], cmp) {
// 			ret = append(ret, xs[i])
// 		}
// 	}
// 	return ret
// }

// // filterIndexes removes indexes that could not point to items in span.
// func filterIndexes[T, Ref any](xs []Index[T, Ref], span state.Span[T], cmp func(a, b T) int) []Index[T, Ref] {
// 	ret := xs[:0]
// 	for i := range xs {
// 		if span.Compare(xs[i], cmp) < 0 {
// 			continue
// 		}
// 		if i+1 < len(xs) && span.Compare(xs[i+1], cmp) > 0 {
// 			continue
// 		}
// 		ret = append(ret, xs[i])
// 	}
// 	return ret
// }
