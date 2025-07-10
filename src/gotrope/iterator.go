package gotrope

import (
	"context"
	"errors"
	"fmt"

	"go.brendoncarroll.net/state"
)

type Span = state.Span[Path]

func PointSpan(x Path) Span {
	return state.PointSpan(x)
}

func TotalSpan() Span {
	return state.TotalSpan[Path]()
}

type Iterator[Ref any] struct {
	s    Storage[Ref]
	root Root[Ref]
	span Span

	offset Weight
	levels []*StreamReader[Ref]
	ctx    context.Context
}

func NewIterator[Ref any](s Storage[Ref], root Root[Ref], span Span) *Iterator[Ref] {
	it := &Iterator[Ref]{
		s:    s,
		root: root,

		levels: make([]*StreamReader[Ref], root.Depth+1),
	}
	for i := range it.levels {
		i := i
		if i == len(it.levels)-1 {
			it.levels[i] = NewStreamReader(s, singleRef(root.Ref))
		} else {
			it.levels[i] = NewStreamReader(s, func(ctx context.Context) (*Ref, error) {
				sr := it.levels[i+1]
				idx, err := readIndex(it.ctx, sr)
				if err != nil {
					return nil, err
				}
				return &idx.Ref, nil
			})
		}
	}
	return it
}

func (it *Iterator[Ref]) Peek(ctx context.Context, ent *Entry) error {
	defer it.setUnsetCtx(ctx)()
	// seek if necessary
	if !it.span.Contains(Path(it.offset), PathCompare) {
		if err := it.Seek(ctx, Path(it.offset)); err != nil {
			return err
		}
	}
	// read one
	var se StreamEntry
	if err := it.levels[0].Peek(ctx, &se); err != nil {
		return err
	}
	ent.set(Path(it.offset), se.Value)
	if !it.span.Contains(ent.Path, PathCompare) {
		return EOS()
	}
	return nil
}

func (it *Iterator[Ref]) Next(ctx context.Context, ent *Entry) error {
	defer it.setUnsetCtx(ctx)()
	// seek if necessary
	if !it.span.Contains(Path(it.offset), PathCompare) {
		if err := it.Seek(ctx, Path(it.offset)); err != nil {
			return err
		}
	}
	// read one
	var se StreamEntry
	if err := it.levels[0].Next(ctx, &se); err != nil {
		return err
	}
	ent.set(Path(it.offset), se.Value)
	if !it.span.Contains(ent.Path, PathCompare) {
		return EOS()
	}
	it.offset.Add(it.offset, se.Weight)
	return nil
}

func (it *Iterator[Ref]) Seek(ctx context.Context, gteq Path) error {
	defer it.setUnsetCtx(ctx)()
	if PathCompare(gteq, Path(it.offset)) < 0 {
		return errors.New("rope.Iterator: cannot seek backwards")
	}
	panic("seek not yet supported")
}

func (it *Iterator[Ref]) setUnsetCtx(ctx context.Context) func() {
	it.ctx = ctx
	return func() { it.ctx = nil }
}

func (it *Iterator[Ref]) syncedBelow() int {
	for i := range it.levels {
		if it.levels[i].Buffered() != 0 {
			return i
		}
	}
	return len(it.levels)
}

// readAt entries have the weight instead of the absolute path.
func (it *Iterator[Ref]) readAt(ctx context.Context, level int, ent *StreamEntry) error {
	if it.syncedBelow() < level {
		panic(fmt.Sprintf("rope.Iterator: read from wrong level %d. synced below %d", level, it.syncedBelow()))
	}
	return it.levels[level].Next(ctx, ent)
}
