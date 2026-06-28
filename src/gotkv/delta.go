package gotkv

import (
	"bytes"
	"context"
	"fmt"

	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

// Delta can be applied to an FS to get another FS
type Delta Root

const (
	deltaEntry_BEGIN = 0
	deltaEntry_END   = 128
)

// 0 => begin
// 255 => end
func deltaEntryType(val []byte) (uint8, error) {
	if len(val) == 0 {
		return 0, fmt.Errorf("invalid key in Delta entry")
	}
	return val[0], nil
}

// Segment is a single entry in a Delta
type Segment struct {
	Span     Span
	Contents Root
}

// deltaEntry is a single entry in a Delta KV stream
type deltaEntry struct {
	Type       uint8
	Begin, End []byte

	// Only one of these lines will be set
	Contents Root
}

func (de deltaEntry) Key(out []byte) []byte {
	switch de.Type {
	case deltaEntry_BEGIN:
		out = append(out, de.Begin...)
	case deltaEntry_END:
		out = append(out, de.End...)
	default:
		panic(de.Type)
	}
	return out
}

func (de deltaEntry) Value(out []byte) []byte {
	out = append(out, de.Type)
	switch de.Type {
	case deltaEntry_BEGIN:
		out = sbe.AppendLP16(out, de.End)
	case deltaEntry_END:
		out = sbe.AppendLP16(out, de.Begin)
	default:
		panic(de.Type)
	}
	out = sbe.AppendLP16(out, de.Contents.Marshal(nil))
	return out
}

type SingleEdit struct {
	Key []byte
}

// DeltaWriter writes a Delta
type DeltaWriter struct {
	m *Machine
	s stores.RW
	// kvb is the Builder for writing DeltaEntries
	kvb *Builder

	// contentb is a builder containing the actual edit contents.
	contentb *Builder

	lastBegin    []byte
	lastEnd      []byte
	lastContents Root

	// haveDeferredEnd is set by AddEdit when a BEGIN has been written
	// but the corresponding END is deferred (to allow combining with touching spans).
	haveDeferredEnd bool
}

func (m *Machine) NewDeltaWriter(s stores.RW) DeltaWriter {
	b := m.NewBuilder(s)
	return DeltaWriter{m: m, s: s, kvb: b}
}

// IsOpen returns true if the last entry written was not an end entry
func (dw *DeltaWriter) IsEditOpen() bool {
	return dw.contentb != nil
}

// LastBegin returns the last begin key.
// The return value should not be modified, and is only valid until the next operation.
func (dw *DeltaWriter) LastBegin() []byte {
	return dw.lastBegin
}

func (dw *DeltaWriter) beginEdit(ctx context.Context, span Span, contents Root) error {
	if dw.IsEditOpen() {
		return fmt.Errorf("there is already an active edit")
	}
	ent := deltaEntry{
		Type:     deltaEntry_BEGIN,
		Begin:    span.Begin,
		End:      span.End,
		Contents: contents,
	}
	if err := dw.kvb.Put(ctx, ent.Key(nil), ent.Value(nil)); err != nil {
		return err
	}
	dw.contentb = dw.m.NewBuilder(dw.s)
	dw.lastBegin = append(dw.lastBegin[:0], span.Begin...)
	dw.lastEnd = append(dw.lastEnd[:0], span.End...)
	dw.lastContents = contents
	return nil
}

func (dw *DeltaWriter) BeginEdit(ctx context.Context, span Span, contents Root) error {
	return dw.beginEdit(ctx, span, contents)
}

// EndEdit writes an end entry
func (dw *DeltaWriter) EndEdit(ctx context.Context, lastKey []byte) error {
	if !dw.IsEditOpen() {
		return fmt.Errorf("no active edit")
	}
	root := dw.lastContents
	if dw.contentb != nil {
		var err error
		addRoot, err := dw.contentb.Finish(ctx)
		if err != nil {
			return fmt.Errorf("finishing content builder: %w", err)
		}
		if !addRoot.Ref.IsZero() {
			root, err = dw.m.Concat(ctx, dw.s, dw.m.NewIterator(dw.s, dw.lastContents, TotalSpan()), dw.m.NewIterator(dw.s, addRoot, TotalSpan()))
			if err != nil {
				return err
			}
		}
	}
	ent := deltaEntry{
		Type:     deltaEntry_END,
		Begin:    dw.lastBegin,
		End:      lastKey,
		Contents: root,
	}
	if err := dw.kvb.Put(ctx, ent.Key(nil), ent.Value(nil)); err != nil {
		return err
	}
	dw.contentb = nil
	dw.lastBegin = dw.lastBegin[:0]
	dw.lastEnd = dw.lastEnd[:0]
	return nil
}

// AddEdit adds an edit to the DeltaWriter
// The Edit Span must be ordered after the last edit.
func (dw *DeltaWriter) AddEdit(ctx context.Context, edit Edit) error {
	if dw.IsEditOpen() {
		return fmt.Errorf("there is already an active edit")
	}
	if dw.haveDeferredEnd {
		switch bytes.Compare(edit.Span.Begin, dw.lastEnd) {
		case -1:
			return fmt.Errorf("overlapping edit span: new span %v starts before last end %q", edit.Span, dw.lastEnd)
		case 0:
			dw.lastEnd = append(dw.lastEnd[:0], edit.Span.End...)
			var err error
			dw.lastContents, err = dw.concatEdit(ctx, dw.lastContents, edit)
			if err != nil {
				return err
			}
			return nil
		case 1:
			if err := dw.writeSegment(ctx); err != nil {
				return err
			}
		}
	}
	b := dw.m.NewBuilder(dw.s)
	for _, ent := range edit.Entries {
		if err := b.Put(ctx, ent.Key, ent.Value); err != nil {
			return err
		}
	}
	editRoot, err := b.Finish(ctx)
	if err != nil {
		return err
	}
	dw.lastBegin = append(dw.lastBegin[:0], edit.Span.Begin...)
	dw.lastEnd = append(dw.lastEnd[:0], edit.Span.End...)
	dw.lastContents = editRoot
	dw.haveDeferredEnd = true
	return nil
}

func (dw *DeltaWriter) writeSegment(ctx context.Context) error {
	beginEnt := deltaEntry{
		Type:     deltaEntry_BEGIN,
		Begin:    dw.lastBegin,
		End:      dw.lastEnd,
		Contents: dw.lastContents,
	}
	if err := dw.kvb.Put(ctx, beginEnt.Key(nil), beginEnt.Value(nil)); err != nil {
		return err
	}
	endEnt := deltaEntry{
		Type:     deltaEntry_END,
		Begin:    dw.lastBegin,
		End:      dw.lastEnd,
		Contents: dw.lastContents,
	}
	return dw.kvb.Put(ctx, endEnt.Key(nil), endEnt.Value(nil))
}

func (dw *DeltaWriter) concatEdit(ctx context.Context, existing Root, edit Edit) (Root, error) {
	b := dw.m.NewBuilder(dw.s)
	for _, ent := range edit.Entries {
		if err := b.Put(ctx, ent.Key, ent.Value); err != nil {
			return Root{}, err
		}
	}
	addRoot, err := b.Finish(ctx)
	if err != nil {
		return Root{}, err
	}
	return dw.m.Concat(ctx, dw.s,
		dw.m.NewIterator(dw.s, existing, TotalSpan()),
		dw.m.NewIterator(dw.s, addRoot, TotalSpan()),
	)
}

func (dw *DeltaWriter) Finish(ctx context.Context) (Delta, error) {
	if dw.IsEditOpen() {
		return Delta{}, fmt.Errorf("cannot finish with open edit")
	}
	if dw.haveDeferredEnd {
		if err := dw.writeSegment(ctx); err != nil {
			return Delta{}, err
		}
		dw.haveDeferredEnd = false
	}
	kvr, err := dw.kvb.Finish(ctx)
	if err != nil {
		return Delta{}, err
	}
	return Delta(kvr), nil
}

var _ streams.Iterator[Segment] = &DeltaReader{}

type DeltaReader struct {
	m *Machine
	s stores.RO
	d Delta

	it        *Iterator
	haveEntry bool
	e         Entry
}

func (m *Machine) NewDeltaReader(s stores.RO, d Delta) *DeltaReader {
	return &DeltaReader{
		m: m,
		s: s,
		d: d,
	}
}

func (di *DeltaReader) Next(ctx context.Context, dst []Segment) (int, error) {
	if di.it == nil {
		di.it = di.m.NewIterator(di.s, Root(di.d), TotalSpan())
	}
	if !di.haveEntry {
		if err := streams.NextUnit(ctx, di.it, &di.e); err != nil {
			return 0, err
		}
		di.haveEntry = true
	}
	typ, err := deltaEntryType(di.e.Value)
	if err != nil {
		return 0, err
	}
	di.haveEntry = false
	if typ != deltaEntry_BEGIN {
		return di.Next(ctx, dst)
	}
	de, err := parseDeltaValue(di.e.Value, typ)
	if err != nil {
		return 0, err
	}
	de.Begin = di.e.Key
	if len(dst) > 0 {
		dst[0] = Segment{
			Span:     Span{Begin: de.Begin, End: de.End},
			Contents: de.Contents,
		}
		return 1, nil
	}
	return 0, nil
}

func parseDeltaValue(val []byte, typ uint8) (deltaEntry, error) {
	de := deltaEntry{Type: typ}
	data := val[1:]
	var field, contentsData []byte
	var err error
	field, data, err = sbe.ReadLP16(data)
	if err != nil {
		return deltaEntry{}, err
	}
	contentsData, _, err = sbe.ReadLP16(data)
	if err != nil {
		return deltaEntry{}, err
	}
	var contents Root
	if err := contents.Unmarshal(contentsData); err != nil {
		return deltaEntry{}, err
	}
	de.Contents = contents
	switch typ {
	case deltaEntry_BEGIN:
		de.End = field
	case deltaEntry_END:
		de.Begin = field
	}
	return de, nil
}

// SegmentFor returns the Segment in d that contains key.
func (m *Machine) SegmentFor(ctx context.Context, s stores.RO, d Delta, key []byte) (Segment, error) {
	iter := m.NewDeltaReader(s, d)
	for {
		var seg Segment
		if err := streams.NextUnit(ctx, iter, &seg); err != nil {
			if streams.IsEOS(err) {
				return Segment{}, fmt.Errorf("no segment contains key %q", key)
			}
			return Segment{}, err
		}
		if seg.Span.Contains(key) {
			return seg, nil
		}
	}
}

func (m *Machine) Apply(ctx context.Context, s stores.RW, base Root, deltas ...Delta) (Root, error) {
	for _, d := range deltas {
		it := m.NewIterator(s, Root(d), TotalSpan())
		for {
			var beginEntry Entry
			if err := streams.NextUnit(ctx, it, &beginEntry); err != nil {
				if streams.IsEOS(err) {
					break
				}
				return Root{}, err
			}
			beginTyp, _ := deltaEntryType(beginEntry.Value)
			if beginTyp != deltaEntry_BEGIN {
				return Root{}, fmt.Errorf("expected BEGIN entry in delta, got type %d", beginTyp)
			}
			be, err := parseDeltaValue(beginEntry.Value, deltaEntry_BEGIN)
			if err != nil {
				return Root{}, err
			}
			be.Begin = beginEntry.Key
			var endEntry Entry
			if err := streams.NextUnit(ctx, it, &endEntry); err != nil {
				return Root{}, fmt.Errorf("expected END entry: %w", err)
			}
			endTyp, _ := deltaEntryType(endEntry.Value)
			if endTyp != deltaEntry_END {
				return Root{}, fmt.Errorf("expected END entry in delta, got type %d", endTyp)
			}
			var entries []Entry
			if err := m.ForEach(ctx, s, be.Contents, TotalSpan(), func(ent Entry) error {
				entries = append(entries, ent.Clone())
				return nil
			}); err != nil {
				return Root{}, err
			}
			edit := Edit{
				Span:    Span{Begin: be.Begin, End: be.End},
				Entries: entries,
			}
			var err2 error
			base, err2 = m.Edit(ctx, s, base, edit)
			if err2 != nil {
				return Root{}, err2
			}
		}
	}
	return base, nil
}
