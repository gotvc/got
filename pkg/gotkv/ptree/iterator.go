package ptree

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/pkg/errors"
)

type Iterator struct {
	s    cadata.Store
	op   *gdat.Operator
	root Root
	span Span
	srs  []*StreamReader
	pos  []byte
}

func NewIterator(s cadata.Store, op *gdat.Operator, root Root, span Span) *Iterator {
	it := &Iterator{
		s:    s,
		op:   op,
		root: root,
		span: span.Clone(),
		srs:  make([]*StreamReader, root.Depth+1),
	}
	it.setPos(span.Start)
	return it
}

func (it *Iterator) Next(ctx context.Context, ent *Entry) error {
	if err := it.initRoot(ctx); err != nil {
		return err
	}
	if err := it.withReader(ctx, 0, func(sr *StreamReader) error {
		return sr.Next(ctx, ent)
	}); err != nil {
		return err
	}
	it.setPosAfter(ent.Key)
	return it.checkAfterSpan(ent)
}

func (it *Iterator) Peek(ctx context.Context, ent *Entry) error {
	if err := it.initRoot(ctx); err != nil {
		return err
	}
	if err := it.withReader(ctx, 0, func(sr *StreamReader) error {
		return sr.Peek(ctx, ent)
	}); err != nil {
		return err
	}
	return it.checkAfterSpan(ent)
}

func (it *Iterator) Seek(ctx context.Context, gteq []byte) error {
	it.setPos(gteq)
	for i := range it.srs {
		it.srs[i] = nil
	}
	return it.initRoot(ctx)
}

func (it *Iterator) withReader(ctx context.Context, i int, fn func(sr *StreamReader) error) error {
	for {
		sr, err := it.getReader(ctx, i)
		if err != nil {
			return err
		}
		if err := fn(sr); err != nil {
			if err == kvstreams.EOS {
				it.srs[i] = nil
				continue
			}
			return err
		} else {
			return nil
		}
	}
}

func (it *Iterator) getReader(ctx context.Context, i int) (*StreamReader, error) {
	if i >= len(it.srs) {
		return nil, kvstreams.EOS
	}
	if it.srs[i] != nil {
		return it.srs[i], nil
	}
	if err := it.withReader(ctx, i+1, func(srAbove *StreamReader) error {
		idxs, err := readIndexes(ctx, srAbove)
		if err != nil {
			return err
		}
		it.srs[i+1] = nil
		it.srs[i] = NewStreamReader(it.s, it.op, idxs)
		if i == 0 {
			return it.srs[i].Seek(ctx, it.pos)
		} else {
			return it.srs[i].SeekIndexes(ctx, it.pos)
		}
	}); err != nil {
		return nil, err
	}
	return it.srs[i], nil
}

func (it *Iterator) checkAfterSpan(ent *Entry) error {
	if it.span.LessThan(ent.Key) {
		return kvstreams.EOS
	}
	return nil
}

func (it *Iterator) setPos(x []byte) {
	it.pos = append(it.pos[:0], x...)
}

func (it *Iterator) setPosAfter(x []byte) {
	it.setPos(x)
	it.pos = append(it.pos, 0x00)
}

func (it *Iterator) initRoot(ctx context.Context) error {
	i := len(it.srs) - 1
	if it.srs[i] != nil {
		return nil
	}
	it.srs[i] = NewStreamReader(it.s, it.op, []Index{rootToIndex(it.root)})
	if i == 0 {
		return it.srs[i].Seek(ctx, it.pos)
	} else {
		return it.srs[i].SeekIndexes(ctx, it.pos)
	}
}

func readIndexes(ctx context.Context, it kvstreams.Iterator) ([]Index, error) {
	var idxs []Index
	if err := kvstreams.ForEach(ctx, it, func(ent Entry) error {
		idx, err := entryToIndex(ent)
		if err != nil {
			return err
		}
		if len(idxs) > 0 {
			prev := idxs[len(idxs)-1].First
			next := idx.First
			if bytes.Compare(prev, next) >= 0 {
				return errors.Errorf("ptree: indexes out of order %q >= %q", prev, next)
			}
		}
		idxs = append(idxs, idx)
		return nil
	}); err != nil {
		return nil, err
	}
	return idxs, nil
}
