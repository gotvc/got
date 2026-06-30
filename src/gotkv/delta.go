package gotkv

import (
	"bytes"
	"context"
	"fmt"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv/kvstreams"
	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

// Delta can be applied to an FS to get another FS
// Delta includes count and total bytes in its marshalled representation.
type Delta Root

func (d Delta) Marshal(out []byte) []byte {
	out = append(out, d.Ref.Marshal()...)
	out = append(out, d.Depth)
	out = sbe.AppendUint64(out, d.Count)
	out = sbe.AppendUint64(out, d.TotalBytes)
	return out
}

func (d *Delta) Unmarshal(data []byte) error {
	// ref
	refData, data, err := sbe.ReadN(data, gdat.RefSize)
	if err != nil {
		return err
	}
	ref, err := gdat.ParseRef(refData)
	if err != nil {
		return err
	}
	if len(data) < 1 {
		return fmt.Errorf("too short to contain depth")
	}
	// depth
	depth := data[0]
	data = data[1:]
	// Count
	count, data, err := sbe.ReadUint64(data)
	if err != nil {
		return err
	}
	// TotalBytes
	totalBytes, data, err := sbe.ReadUint64(data)
	if err != nil {
		return err
	}

	d.Ref = ref
	d.Depth = depth
	d.Count = count
	d.TotalBytes = totalBytes
	d.First = append(d.First[:0], data...)
	return nil
}

const (
	deltaEntry_BEGIN = (1 << 7)
	deltaEntry_END   = (1 << 6)
	deltaEntry_PIVOT = deltaEntry_BEGIN | deltaEntry_END
	// UNIT is not yet used.
	deltaEntry_UNIT = 0
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
	Type              uint8
	Begin, Pivot, End []byte

	// Prev is set for END and PIVOT entries
	Prev Root
	// Next is set for BEGIN and PIVOT entries
	Next Root
}

func (de deltaEntry) Key(out []byte) []byte {
	switch de.Type {
	case deltaEntry_BEGIN:
		out = append(out, de.Begin...)
	case deltaEntry_END:
		out = append(out, de.End...)
	case deltaEntry_PIVOT:
		out = append(out, de.Pivot...)
	default:
		panic(de.Type)
	}
	return out
}

func (de deltaEntry) Value(out []byte) []byte {
	out = append(out, de.Type)
	switch de.Type {
	case deltaEntry_BEGIN:
		out = sbe.AppendLP16(out, de.End)               // matches the END entry key
		out = sbe.AppendLP16(out, de.Next.Marshal(nil)) // matches END content
	case deltaEntry_END:
		out = sbe.AppendLP16(out, de.Begin)             // matches BEGIN entry key
		out = sbe.AppendLP16(out, de.Prev.Marshal(nil)) // matches BEGIN content
	case deltaEntry_PIVOT:
		out = sbe.AppendLP16(out, de.End)               // this will correspond to the prev edit
		out = sbe.AppendLP16(out, de.Begin)             // this will correspond to the next edit
		out = sbe.AppendLP16(out, de.Prev.Marshal(nil)) // matches BEGIN content
		out = sbe.AppendLP16(out, de.Next.Marshal(nil)) // matches END content
	default:
		panic(de.Type)
	}
	return out
}

func (de deltaEntry) Contents() Root {
	switch de.Type {
	case deltaEntry_BEGIN:
		return de.Next
	case deltaEntry_END:
		return de.Prev
	default:
		panic(de.Type)
	}
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
	switch typ {
	case deltaEntry_BEGIN:
		de.End = field
		de.Next = contents
	case deltaEntry_END:
		de.Begin = field
		de.Prev = contents
	}
	return de, nil
}

func parsePivot(val []byte) (deltaEntry, error) {
	de := deltaEntry{Type: deltaEntry_PIVOT}
	d := val[1:]
	var err error
	var prevData, nextData []byte
	de.End, d, err = sbe.ReadLP16(d)
	if err != nil {
		return deltaEntry{}, err
	}
	de.Begin, d, err = sbe.ReadLP16(d)
	if err != nil {
		return deltaEntry{}, err
	}
	prevData, d, err = sbe.ReadLP16(d)
	if err != nil {
		return deltaEntry{}, err
	}
	nextData, _, err = sbe.ReadLP16(d)
	if err != nil {
		return deltaEntry{}, err
	}
	if err := de.Prev.Unmarshal(prevData); err != nil {
		return deltaEntry{}, err
	}
	if err := de.Next.Unmarshal(nextData); err != nil {
		return deltaEntry{}, err
	}
	return de, nil
}

// DeltaWriter writes a Delta
// It is not thread-safe
type DeltaWriter struct {
	m *Machine
	s stores.RW

	// kvb is the Builder for writing DeltaEntries
	kvb *Builder
	// lastEnd is the key for the last END entry that was written.
	lastEnd []byte
	// pendingSegs holds segments that cannot be written as delta entries yet.
	pendingSegs []Segment

	// edit holds the active edit
	edit struct {
		// begin is the key passed to the last BeginEdit call.
		begin []byte
		// end is the end of the edited range exclusively.
		end []byte
		// contentb is a builder containing the actual edit contents.
		// it buffers operations until
		contentb *Builder
	}
}

func (m *Machine) NewDeltaWriter(s stores.RW) DeltaWriter {
	b := m.NewBuilder(s)
	return DeltaWriter{m: m, s: s, kvb: b}
}

func (dw *DeltaWriter) Finish(ctx context.Context) (Delta, error) {
	if dw.IsEditOpen() {
		return Delta{}, fmt.Errorf("cannot finish with open edit")
	}
	if err := dw.flushSegments(ctx); err != nil {
		return Delta{}, err
	}
	kvr, err := dw.kvb.Finish(ctx)
	if err != nil {
		return Delta{}, err
	}
	return Delta(kvr), nil
}

// writeSegment appends seg to pendingSegs.
// If seg is not adjacent to the last pending segment, all pending segments
// are flushed first.
func (dw *DeltaWriter) writeSegment(ctx context.Context, seg Segment) error {
	if len(dw.pendingSegs) > 0 {
		last := dw.pendingSegs[len(dw.pendingSegs)-1]
		if !bytes.Equal([]byte(last.Span.End), []byte(seg.Span.Begin)) {
			if err := dw.flushSegments(ctx); err != nil {
				return err
			}
		}
	}
	dw.pendingSegs = append(dw.pendingSegs, seg)
	return nil
}

func (dw *DeltaWriter) flushSegments(ctx context.Context) error {
	segs := dw.pendingSegs
	if len(segs) == 0 {
		return nil
	}
	dw.pendingSegs = dw.pendingSegs[:0]

	first := segs[0]
	be := deltaEntry{
		Type:  deltaEntry_BEGIN,
		Begin: first.Span.Begin,
		End:   first.Span.End,
		Next:  first.Contents,
	}
	if err := dw.kvb.Put(ctx, be.Key(nil), be.Value(nil)); err != nil {
		return err
	}

	for i := 1; i < len(segs); i++ {
		prev := segs[i-1]
		cur := segs[i]
		pivot := deltaEntry{
			Type:  deltaEntry_PIVOT,
			Pivot: cur.Span.Begin,
			End:   prev.Span.Begin,
			Begin: cur.Span.End,
			Prev:  prev.Contents,
			Next:  cur.Contents,
		}
		if err := dw.kvb.Put(ctx, pivot.Key(nil), pivot.Value(nil)); err != nil {
			return err
		}
	}

	last := segs[len(segs)-1]
	ee := deltaEntry{
		Type:  deltaEntry_END,
		End:   []byte(last.Span.End),
		Begin: []byte(last.Span.Begin),
		Prev:  last.Contents,
	}
	if err := dw.kvb.Put(ctx, ee.Key(nil), ee.Value(nil)); err != nil {
		return err
	}
	dw.lastEnd = []byte(last.Span.End)
	return nil
}

// IsOpen returns true if the last entry written was not an end entry
func (dw *DeltaWriter) IsEditOpen() bool {
	return dw.edit.contentb != nil
}

// BeginEdit starts a new edit, recording a contiguous region of the keyspace.
// begin is the first key in the current editing span.
func (dw *DeltaWriter) BeginEdit(ctx context.Context, begin []byte) error {
	if dw.IsEditOpen() {
		return fmt.Errorf("there is already an active edit")
	}
	if !dw.CanEdit(begin) {
		return fmt.Errorf("edit begin %q overlaps with already written region", begin)
	}
	dw.edit.begin = append(dw.edit.begin[:0], begin...)
	dw.edit.contentb = dw.m.NewBuilder(dw.s)
	dw.edit.end = append(dw.edit.end[:0], begin...)
	return nil
}

// Put writes to the content entries Builder for the current edit.
func (dw *DeltaWriter) Put(ctx context.Context, key, value []byte) error {
	if !dw.IsEditOpen() {
		return fmt.Errorf("cannot put entry, there is no active edit")
	}
	return dw.edit.contentb.Put(ctx, key, value)
}

// DeleteUntil deletes a contiguous Span of the keyspace, up until, but not including endExcl
func (dw *DeltaWriter) DeleteUntil(ctx context.Context, endExcl []byte) error {
	if !dw.IsEditOpen() {
		return fmt.Errorf("cannot delete span, there is no active edit")
	}
	if !dw.CanEdit(endExcl) {
		return fmt.Errorf("span intersects with already written region")
	}
	dw.edit.end = endExcl
	return nil
}

// EndEdit ends the current edit, and marks the last key affected.
func (dw *DeltaWriter) EndEdit(ctx context.Context, endExcludingKey []byte) error {
	if !dw.IsEditOpen() {
		return fmt.Errorf("cannot end edit.  there is no active edit")
	}
	if bytes.Compare(dw.edit.end, endExcludingKey) > 0 {
		return fmt.Errorf("cannot use endKey %q, have already edited %v", endExcludingKey, dw.edit.end)
	}
	root, err := dw.edit.contentb.Finish(ctx)
	if err != nil {
		return err
	}
	dw.edit.contentb = nil
	span := kvstreams.CloneSpan(Span{
		Begin: dw.edit.begin,
		End:   endExcludingKey,
	})
	return dw.writeSegment(ctx, Segment{Span: span, Contents: root})
}

// CanEdit returns true if a key is editable.
// A key can be edited (either put or delete) if it has not yet been included
// in a span written out.
func (dw *DeltaWriter) CanEdit(k []byte) bool {
	if bytes.Compare(k, dw.lastEnd) < 0 {
		return false
	}
	if len(dw.pendingSegs) > 0 {
		lastSeg := dw.pendingSegs[len(dw.pendingSegs)-1]
		if bytes.Compare(k, []byte(lastSeg.Span.End)) < 0 {
			return false
		}
	}
	return true
}

// Edit adds an edit to the DeltaWriter
// The Edit Span must be ordered after the last edit.
func (dw *DeltaWriter) Edit(ctx context.Context, edit Edit) error {
	if dw.IsEditOpen() {
		return fmt.Errorf("there is already an active edit")
	}
	if err := dw.BeginEdit(ctx, edit.Span.Begin); err != nil {
		return err
	}
	for _, ent := range edit.Entries {
		if err := dw.Put(ctx, ent.Key, ent.Value); err != nil {
			return err
		}
	}
	return dw.EndEdit(ctx, edit.Span.End)
}

var _ streams.Iterator[Segment] = &DeltaReader{}

type DeltaReader struct {
	m *Machine
	s stores.RW
	d Delta

	it         *Iterator
	chainBegin []byte
	chainEnd   []byte
	chainEnts  []Entry

	buf    [2]deltaEntry
	bufLen int
}

func (m *Machine) NewDeltaReader(s stores.RW, d Delta) *DeltaReader {
	return &DeltaReader{
		m: m,
		s: s,
		d: d,
	}
}

// Next reads until it has a beginning and pivot/end for a Segment and then emits it.
func (di *DeltaReader) Next(ctx context.Context, dst []Segment) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if di.it == nil {
		di.it = di.m.NewIterator(di.s, Root(di.d), TotalSpan())
	}
	if di.bufLen == 1 {
		var ent Entry
		if err := streams.NextUnit(ctx, di.it, &ent); err != nil {
			return 0, err
		}
		typ, err := deltaEntryType(ent.Value)
		if err != nil {
			return 0, err
		}
		switch typ {
		case deltaEntry_END:
			ee, err := parseDeltaValue(ent.Value, deltaEntry_END)
			if err != nil {
				return 0, err
			}
			ee.End = ent.Key
			if !bytes.Equal(di.buf[0].End, ent.Key) {
				return 0, fmt.Errorf("END key %q does not match BEGIN End %q", ent.Key, di.buf[0].End)
			}
			if !di.buf[0].Next.Equal(ee.Prev) {
				return 0, fmt.Errorf("END Prev does not match BEGIN Next")
			}
			di.appendEntries(ctx, di.buf[0].Contents())
			dst[0] = di.emitMerged(ctx, ee.End)
			return 1, nil

		case deltaEntry_PIVOT:
			pv, err := parsePivot(ent.Value)
			if err != nil {
				return 0, err
			}
			pv.Pivot = ent.Key
			if !bytes.Equal(di.buf[0].End, ent.Key) {
				return 0, fmt.Errorf("PIVOT key %q does not match BEGIN End %q", ent.Key, di.buf[0].End)
			}
			if !di.buf[0].Next.Equal(pv.Prev) {
				return 0, fmt.Errorf("PIVOT Prev does not match BEGIN Next")
			}
			di.appendEntries(ctx, pv.Prev)
			di.chainEnd = pv.Begin
			di.buf[0].Type = deltaEntry_BEGIN
			di.buf[0].Begin = pv.Pivot
			di.buf[0].End = pv.Begin
			di.buf[0].Next = pv.Next
			return di.Next(ctx, dst)

		default:
			return 0, fmt.Errorf("expected END or PIVOT after BEGIN, got type %d", typ)
		}
	}

	var ent Entry
	if err := streams.NextUnit(ctx, di.it, &ent); err != nil {
		return 0, err
	}
	typ, err := deltaEntryType(ent.Value)
	if err != nil {
		return 0, err
	}
	if typ != deltaEntry_BEGIN {
		return 0, fmt.Errorf("expected BEGIN entry, got type %d", typ)
	}
	be, err := parseDeltaValue(ent.Value, deltaEntry_BEGIN)
	if err != nil {
		return 0, err
	}
	be.Begin = ent.Key
	di.buf[0] = be
	di.bufLen = 1
	di.chainBegin = be.Begin
	di.chainEnd = be.End
	di.chainEnts = di.chainEnts[:0]
	return di.Next(ctx, dst)
}

func (di *DeltaReader) appendEntries(ctx context.Context, root Root) {
	_ = di.m.ForEach(ctx, di.s, root, TotalSpan(), func(ent Entry) error {
		di.chainEnts = append(di.chainEnts, ent.Clone())
		return nil
	})
}

func (di *DeltaReader) emitMerged(ctx context.Context, end []byte) Segment {
	b := di.m.NewBuilder(di.s)
	for _, e := range di.chainEnts {
		_ = b.Put(ctx, e.Key, e.Value)
	}
	root, _ := b.Finish(ctx)
	di.bufLen = 0
	return Segment{
		Span: Span{
			Begin: di.chainBegin,
			End:   end,
		},
		Contents: root,
	}
}

// SegmentFor returns the Segment in d that contains key.
func (m *Machine) SegmentFor(ctx context.Context, s stores.RW, d Delta, key []byte) (Segment, error) {
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
			if err := m.ForEach(ctx, s, be.Contents(), TotalSpan(), func(ent Entry) error {
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
