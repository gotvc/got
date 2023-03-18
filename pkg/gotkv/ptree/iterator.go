package ptree

import (
	"bytes"
	"context"
	"fmt"

	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

type Iterator[Ref any] struct {
	p IteratorParams[Ref]

	levels [][]Entry
	pos    []byte
}

type IteratorParams[Ref any] struct {
	Store      Getter[Ref]
	ParseRef   func([]byte) (Ref, error)
	AppendRef  func([]byte, Ref) []byte
	NewDecoder func() Decoder
	Compare    CompareFunc

	Root Root[Ref]
	Span Span
}

func NewIterator[Ref any](params IteratorParams[Ref]) *Iterator[Ref] {
	it := &Iterator[Ref]{
		p: params,

		levels: make([][]Entry, params.Root.Depth+2),
	}
	it.levels[it.p.Root.Depth+1] = []Entry{indexToEntry(rootToIndex(it.p.Root), it.p.AppendRef)}
	it.setPos(params.Span.Begin)
	return it
}

func (it *Iterator[Ref]) Next(ctx context.Context, ent *Entry) error {
	return it.next(ctx, 0, ent)
}

func (it *Iterator[Ref]) Peek(ctx context.Context, ent *Entry) error {
	return it.peek(ctx, 0, ent)
}

func (it *Iterator[Ref]) Seek(ctx context.Context, gteq []byte) error {
	it.levels[it.p.Root.Depth+1] = []Entry{indexToEntry(rootToIndex(it.p.Root), it.p.AppendRef)}
	it.setPos(gteq)
	for i := len(it.levels) - 1; i >= 0; i-- {
		if i == 0 {
			it.levels[i] = filterEntries(it.levels[i], it.getSpan())
		} else {
			it.levels[i] = filterIndexes(it.levels[i], it.getSpan())
		}
	}
	return nil
}

func (it *Iterator[Ref]) next(ctx context.Context, level int, ent *Entry) error {
	if it.syncLevel() < level {
		return fmt.Errorf("cannot read from level %d, only synced to %d", level, it.syncLevel())
	}
	entries, err := it.getEntries(ctx, level)
	if err != nil {
		return err
	}
	ent2 := entries[0]
	ent.Key = append(ent.Key[:0], ent2.Key...)
	ent.Value = append(ent.Value[:0], ent2.Value...)
	it.advanceLevel(level, true)
	return nil
}

func (it *Iterator[Ref]) peek(ctx context.Context, level int, ent *Entry) error {
	if it.syncLevel() < level {
		return fmt.Errorf("cannot read from level %d, only synced to %d", level, it.syncLevel())
	}
	entries, err := it.getEntries(ctx, level)
	if err != nil {
		return err
	}
	ent2 := entries[0]
	ent.Key = append(ent.Key[:0], ent2.Key...)
	ent.Value = append(ent.Value[:0], ent2.Value...)
	return nil
}

func (it *Iterator[Ref]) getEntries(ctx context.Context, level int) ([]Entry, error) {
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
		idx, err := entryToIndex(above[0], it.p.ParseRef)
		if err != nil {
			return nil, fmt.Errorf("converting entry to index at level %d: %w", level, err)
		}
		it.advanceLevel(level+1, false)
		ents, err := ListEntries(ctx, ReadParams[Ref]{Store: it.p.Store, Compare: it.p.Compare, NewDecoder: it.p.NewDecoder, ParseRef: it.p.ParseRef}, idx)
		if err != nil {
			return nil, err
		}
		if level == 0 {
			ents = filterEntries(ents, it.getSpan())
		} else {
			ents = filterIndexes(ents, it.getSpan())
		}
		if len(ents) > 0 {
			it.levels[level] = ents
			return it.levels[level], nil
		}
	}
}

func (it *Iterator[Ref]) syncLevel() int {
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
		if len(it.levels[i]) > 1 && (it.p.Span.End == nil || bytes.Compare(it.levels[i][1].Key, it.p.Span.End) < 0) {
			break
		}
	}
	return min(bot, top)
}

func (it *Iterator[Ref]) advanceLevel(level int, updatePos bool) {
	entries := it.levels[level]
	it.levels[level] = entries[1:]
	if !updatePos {
		return
	}
	for i := level; i < len(it.levels); i++ {
		entries := it.levels[i]
		if len(entries) > 0 {
			it.setPos(entries[0].Key)
			return
		}
	}
	it.pos = nil // end of the stream
}

func (it *Iterator[Ref]) setPos(x []byte) {
	it.pos = append(it.pos[:0], x...)
}

func (it *Iterator[Ref]) getSpan() Span {
	return Span{
		Begin: it.pos,
		End:   it.p.Span.End,
	}
}

func filterEntries(xs []Entry, span Span) []Entry {
	ret := xs[:0]
	for i := range xs {
		if span.Contains(xs[i].Key) {
			ret = append(ret, xs[i])
		}
	}
	return ret
}

// filterIndexes removes indexes that could not point to items in span.
func filterIndexes(xs []Entry, span Span) []Entry {
	ret := xs[:0]
	for i := range xs {
		if span.AllLt(xs[i].Key) {
			continue
		}
		if i+1 < len(xs) && bytes.Compare(span.Begin, xs[i+1].Key) >= 0 {
			continue
		}
		ret = append(ret, xs[i])
	}
	return ret
}
