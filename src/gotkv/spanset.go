package gotkv

import (
	"bytes"
	"context"

	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

// SpanSet is an ordered set of non-overlapping Spans
type SpanSet Root

// SSEntry is a ReadSet entry
type SSEntry struct {
	Type  uint8
	Begin []byte
	End   []byte
}

func (rse SSEntry) Key(out []byte) []byte {
	switch rse.Type {
	case deltaEntry_BEGIN:
		out = append(out, rse.Begin...)
	case deltaEntry_END:
		out = append(out, rse.End...)
	default:
		panic(rse.Type)
	}
	return out
}

func (rse SSEntry) Value(out []byte) []byte {
	switch rse.Type {
	case deltaEntry_BEGIN:
		out = sbe.AppendLP16(out, rse.End)
	case deltaEntry_END:
		out = sbe.AppendLP16(out, rse.Begin)
	default:
		panic(rse.Type)
	}
	return out
}

type SpanSetWriter struct {
	b        *Builder
	haveSpan bool
	lastSpan Span
}

func (m *Machine) NewSpanSetWriter(s stores.RW) SpanSetWriter {
	return SpanSetWriter{b: m.NewBuilder(s)}
}

// Add adds a Span to the Set. It may overlap with the last span
// But it cannot start before the last span.
// If this span is totally contained in the last one, then this is a no-op.
func (rsw *SpanSetWriter) Add(ctx context.Context, span Span) error {
	if rsw.haveSpan {
		if spanOverlaps(rsw.lastSpan, span) || bytes.Equal(rsw.lastSpan.End, span.Begin) {
			if bytes.Compare(span.End, rsw.lastSpan.End) > 0 {
				rsw.lastSpan.End = span.End
			}
			return nil
		}
		beginEnt := SSEntry{Type: deltaEntry_BEGIN, Begin: rsw.lastSpan.Begin, End: rsw.lastSpan.End}
		if err := rsw.b.Put(ctx, beginEnt.Key(nil), beginEnt.Value(nil)); err != nil {
			return err
		}
		endEnt := SSEntry{Type: deltaEntry_END, Begin: rsw.lastSpan.Begin, End: rsw.lastSpan.End}
		if err := rsw.b.Put(ctx, endEnt.Key(nil), endEnt.Value(nil)); err != nil {
			return err
		}
	}
	rsw.lastSpan = Span{Begin: span.Begin, End: span.End}
	rsw.haveSpan = true
	return nil
}

// Finish adds an end entry if necessary and then returns the root of the SpanSet.
func (rsw *SpanSetWriter) Finish(ctx context.Context) (SpanSet, error) {
	if rsw.haveSpan {
		beginEnt := SSEntry{Type: deltaEntry_BEGIN, Begin: rsw.lastSpan.Begin, End: rsw.lastSpan.End}
		if err := rsw.b.Put(ctx, beginEnt.Key(nil), beginEnt.Value(nil)); err != nil {
			return SpanSet{}, err
		}
		endEnt := SSEntry{Type: deltaEntry_END, Begin: rsw.lastSpan.Begin, End: rsw.lastSpan.End}
		if err := rsw.b.Put(ctx, endEnt.Key(nil), endEnt.Value(nil)); err != nil {
			return SpanSet{}, err
		}
	}
	root, err := rsw.b.Finish(ctx)
	return SpanSet(root), err
}

// SpansOverlap returns true if any of the spans in ss overlap x.
func (m *Machine) SpansOverlap(ctx context.Context, s stores.RO, ss SpanSet, x Span) (bool, error) {
	it := m.NewIterator(s, Root(ss), TotalSpan())
	for {
		var beginEntry Entry
		if err := streams.NextUnit(ctx, it, &beginEntry); err != nil {
			if streams.IsEOS(err) {
				return false, nil
			}
			return false, err
		}
		end, _, err := sbe.ReadLP16(beginEntry.Value)
		if err != nil {
			return false, err
		}
		span := Span{Begin: beginEntry.Key, End: end}
		if spanOverlaps(span, x) {
			return true, nil
		}
		var endEntry Entry
		if err := streams.NextUnit(ctx, it, &endEntry); err != nil {
			return false, err
		}
	}
}

func spanOverlaps(a, b Span) bool {
	if a.End != nil && bytes.Compare(a.End, b.Begin) <= 0 {
		return false
	}
	if b.End != nil && bytes.Compare(b.End, a.Begin) <= 0 {
		return false
	}
	return true
}