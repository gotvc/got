package ptree

import (
	"context"
	"errors"
	"fmt"

	"github.com/brendoncarroll/go-state"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

type Iterator[T, Ref any] struct {
	p IteratorParams[T, Ref]

	levels []iterLevel[T, Ref]
	span   state.Span[T]
}

type iterLevel[T, Ref any] struct {
	entries []T
	indexes []Index[T, Ref]
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

		levels: make([]iterLevel[T, Ref], params.Root.Depth+2),
		span:   params.Span,
	}
	it.levels[it.p.Root.Depth+1] = iterLevel[T, Ref]{
		indexes: []Index[T, Ref]{rootToIndex(it.p.Root)},
	}
	return it
}

func (it *Iterator[T, Ref]) Next(ctx context.Context, ent *T) error {
	return it.next(ctx, 0, dual[T, Ref]{})
}

func (it *Iterator[T, Ref]) Peek(ctx context.Context, ent *T) error {
	return it.peek(ctx, 0, ent)
}

func (it *Iterator[T, Ref]) Seek(ctx context.Context, gteq T) error {
	if it.span.Compare(gteq, it.p.Compare) > 0 {
		return errors.New("cannot seek backwards")
	}
	it.span = it.span.WithLowerIncl(gteq)
	for i := len(it.levels) - 1; i >= 0; i-- {
		if i == 0 {
			it.levels[i].entries = filterEntries(it.levels[i].entries, it.span, it.p.Compare)
		} else {
			it.levels[i].indexes = filterIndexes(it.levels[i].indexes, it.span, it.p.Compare)
		}
	}
	return nil
}

func (it *Iterator[T, Ref]) next(ctx context.Context, level int, dst dual[T, Ref]) error {
	if it.syncLevel() < level {
		return fmt.Errorf("cannot read from level %d, only synced to %d", level, it.syncLevel())
	}
	entries, err := it.getEntries(ctx, level)
	if err != nil {
		return err
	}
	it.copy(ent, entries[0])
	it.advanceLevel(level, true)
	return nil
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

	} else if il.indexes != nil {
		it.copy(il.indexes[])
	} 
	if len(il.)
	it.copy(ent, entries[0])
	return nil
}

func (it *Iterator[T, Ref]) getLevel(ctx context.Context, level int) (iterLevel[T, Ref], error) {
	if level >= len(it.levels) {
		return nil, kvstreams.EOS
	}
	if len(it.levels[level]) > 0 {
		return it.levels[level], nil
	}
	for {
		above, err := it.getEntries(ctx, level+1)
		if err != nil {
			return nil, err
		}
		idx, err := it.convertEntry(above[0])
		if err != nil {
			return nil, fmt.Errorf("converting entry to index at level %d: %w", level, err)
		}
		it.advanceLevel(level+1, false)
		ents, err := ListEntries(ctx, ReadParams[T, Ref]{Store: it.s, Compare: it.compare, NewDecoder: it.newDecoder, ConvertEntry: it.convertEntry}, idx)
		if err != nil {
			return nil, err
		}
		if level == 0 {
			ents = filterEntries(ents, it.span, it.compare)
		} else {
			ents = filterIndexes(ents, it.span, it.compare)
		}
		if len(ents) > 0 {
			it.levels[level] = ents
			return it.levels[level], nil
		}
	}
}

func (it *Iterator[T, Ref]) syncLevel() int {
	// bot is the index below which all levels are synced
	var bot int
	for i := range it.levels {
		bot = i
		if len(it.levels[i]) > 0 {
			break
		}
	}
	// top is maximum index where the level has more than 1 entry
	// top is required because indexes at the right most side of the tree cannot be copied
	// since they could point to incomplete nodes.
	// the iterator's span causes us to consider some otherwise complete nodes incomplete.
	var top int
	for i := len(it.levels) - 1; i >= 0; i-- {
		top = i
		if len(it.levels[i]) > 1 && it.span.Contains(it.levels[i][1], it.compare) {
			break
		}
	}
	return min(bot, top)
}

func (it *Iterator[T, Ref]) advanceLevel(level int, updatePos bool) {
	entries := it.levels[level]
	it.levels[level] = entries[1:]
	if !updatePos {
		return
	}
	for i := level; i < len(it.levels); i++ {
		il := it.levels[i]
		if len(il.entries) > 0 {
			it.setGt(il.entries[0])
			return
		} else if len(il.indexes) > 0 {
			// TODO
			//it.setGt(il.indexes[0])
			return
		}
	}
	// end of the stream
}

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

func filterEntries[T any](xs []T, span state.Span[T], cmp func(a, b T) int) []T {
	ret := xs[:0]
	for i := range xs {
		if span.Contains(xs[i], cmp) {
			ret = append(ret, xs[i])
		}
	}
	return ret
}

// filterIndexes removes indexes that could not point to items in span.
func filterIndexes[T, Ref any](xs []Index[T, Ref], span state.Span[T], cmp func(a, b T) int) []Index[T, Ref] {
	ret := xs[:0]
	for i := range xs {
		if span.Compare(xs[i], cmp) < 0 {
			continue
		}
		if i+1 < len(xs) && span.Compare(xs[i+1], cmp) > 0 {
			continue
		}
		ret = append(ret, xs[i])
	}
	return ret
}
