// Package gotfsdelta implements a delta format for sets of changes to GotFS filesystems
package gotfsdelta

import (
	"bytes"
	"context"
	"fmt"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotkv/gotkvdelta"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/slices2"
	"go.brendoncarroll.net/exp/streams"
)

type (
	Root   = gotfs.Root
	Extent = gotfs.Extent
	Entry  = gotfs.Entry
	Key    = gotfs.Key
	Value  = gotfs.Value
	Info   = gotfs.Info
)

type Machine struct {
	gotfs *gotfs.Machine
	gotkv *gotkv.Machine
	kvd   gotkvdelta.Machine
}

func NewMachine(fsmach *gotfs.Machine, salt [32]byte) Machine {
	kvmach := gotkv.NewMachine(gotkv.Params{
		Salt:     salt,
		MeanSize: 1 << 16,
		MaxSize:  stores.MaxSize,
	})
	kvdmach := gotkvdelta.NewMachine(&kvmach)
	return Machine{gotfs: fsmach, gotkv: &kvmach, kvd: kvdmach}
}

type Delta gotkvdelta.Delta

// DeltaWriter writes a stream of GotFS filesystem edits to a store
type DeltaWriter struct {
	s stores.RW

	kvw        gotkvdelta.Writer
	differPath string
}

func (m *Machine) NewDeltaWriter(s stores.RW) DeltaWriter {
	return DeltaWriter{
		s:   s,
		kvw: m.kvd.NewWriter(s),
	}
}

func (dw *DeltaWriter) Finish(ctx context.Context) (Delta, error) {
	if err := dw.closeExtentEdit(ctx); err != nil {
		return Delta{}, err
	}
	d, err := dw.kvw.Finish(ctx)
	if err != nil {
		return Delta{}, err
	}
	return Delta(d), nil
}

func (dw *DeltaWriter) Push(ctx context.Context, seg gotfs.Segment) error {
	seg2 := seg.ToSegment()
	return dw.kvw.Push(ctx, seg2)
}

// PutAllFileData replaces the file data extents for path p.
func (dw *DeltaWriter) PutAllFileData(ctx context.Context, p string, exts []Extent) error {
	ik, err := gotfs.NewInfoKey(p)
	if err != nil {
		return err
	}
	prefix := ik.Prefix(nil)
	prefixEnd := gotkv.PrefixEnd(prefix)

	if err := dw.kvw.BeginEdit(ctx, prefix); err != nil {
		return err
	}
	var cum uint64
	for _, ext := range exts {
		cum += uint64(ext.Length)
		ek := gotfs.NewExtentKey(p, cum)
		v := Value{Extent: ext}
		if err := dw.kvw.Put(ctx, ek.Marshal(nil), v.Marshal(false, nil)); err != nil {
			return err
		}
	}
	return dw.kvw.EndEdit(ctx, prefixEnd)
}

// PutInfo writes a metadata entry for path p.
// An Info change is a single-item edit.
func (dw *DeltaWriter) PutInfo(ctx context.Context, p string, info Info) error {
	ik, err := gotfs.NewInfoKey(p)
	if err != nil {
		return err
	}
	keyBytes := ik.Marshal(nil)

	v := Value{Info: info}
	valBytes := v.Marshal(true, nil)

	if err := dw.kvw.BeginEdit(ctx, keyBytes); err != nil {
		return err
	}
	if err := dw.kvw.Put(ctx, keyBytes, valBytes); err != nil {
		return err
	}
	return dw.kvw.EndEdit(ctx, gotkv.KeyAfter(keyBytes))
}

// DeletePath deletes the info at p, any extents if it is a regular file
// and all of the child directories if it is a directory.
func (dw *DeltaWriter) DeletePath(ctx context.Context, p string) error {
	ik, err := gotfs.NewInfoKey(p)
	if err != nil {
		return err
	}
	span := ik.ChildrenSpan()

	if err := dw.kvw.BeginEdit(ctx, span.Begin); err != nil {
		return err
	}
	if err := dw.kvw.DeleteUntil(ctx, span.End); err != nil {
		return err
	}
	return dw.kvw.EndEdit(ctx, span.End)
}

// ReadFromDiffer reads all the entries from the differ stream and adds them to the delta.
func (dw *DeltaWriter) ReadFromDiffer(ctx context.Context, dfr *gotfs.Differ) error {
	return streams.ForEach(ctx, dfr, func(dent gotfs.DiffEntry) error {
		p := dent.Key.Path()
		rok := dent.Right.Ok
		if dent.Key.IsInfo() {
			if err := dw.closeExtentEdit(ctx); err != nil {
				return err
			}
			if dent.IsDelete() {
				return dw.DeletePath(ctx, p)
			} else {
				return dw.PutInfo(ctx, p, dent.Right.X.Info)
			}
		}
		if !rok {
			return nil
		}
		if dw.differPath != p {
			if err := dw.closeExtentEdit(ctx); err != nil {
				return err
			}
			ik, err := gotfs.NewInfoKey(p)
			if err != nil {
				return err
			}
			begin := infoKeyNext(ik.Marshal(nil))
			if err := dw.kvw.BeginEdit(ctx, begin); err != nil {
				return err
			}
			dw.differPath = p
		}
		v := Value{Extent: dent.Right.X.Extent}
		return dw.kvw.Put(ctx, dent.Key.Marshal(nil), v.Marshal(false, nil))
	})
}

func (dw *DeltaWriter) closeExtentEdit(ctx context.Context) error {
	if dw.differPath == "" {
		return nil
	}
	p := dw.differPath
	dw.differPath = ""
	ik, err := gotfs.NewInfoKey(p)
	if err != nil {
		return err
	}
	end := gotkv.PrefixEnd(ik.Prefix(nil))
	return dw.kvw.EndEdit(ctx, end)
}

var _ streams.Iterator[gotfs.Segment] = &DeltaReader{}

type DeltaReader struct {
	s  stores.RO
	d  Delta
	dr gotkvdelta.Reader
}

func (m *Machine) NewDeltaReader(s stores.RO, d Delta) DeltaReader {
	return DeltaReader{
		s:  s,
		d:  d,
		dr: m.kvd.NewReader(s, gotkvdelta.Delta(d)),
	}
}

func (dw *DeltaReader) Next(ctx context.Context, dst []gotfs.Segment) (int, error) {
	dst2 := make([]gotkvdelta.Segment, len(dst))
	for i := range dst2 {
		dst2[i] = dst[i].ToSegment()
	}
	n, err := dw.dr.Next(ctx, dst2)
	if err != nil {
		return 0, err
	}
	for i := range dst[:n] {
		span, err := gotfs.NewSpan(dst2[i].Span)
		if err != nil {
			return 0, err
		}
		dst[i] = gotfs.Segment{
			Span:     span,
			Contents: dst2[i].Contents,
		}
	}
	return n, nil
}

func infoKeyNext(infoKey []byte) []byte {
	out := make([]byte, len(infoKey))
	copy(out, infoKey)
	out[len(out)-1] = 1
	return out
}

// Apply applies a delta to the root, producing a new root
func (m *Machine) Apply(ctx context.Context, ss gotfs.RW, root Root, d Delta) (Root, error) {
	dr := m.kvd.NewReader(ss.Metadata, gotkvdelta.Delta(d))
	b := m.gotfs.NewBuilder(ctx, ss)
	var lastEnd []byte
	if err := streams.ForEach(ctx, &dr, func(seg gotkvdelta.Segment) error {
		cmp := bytes.Compare(lastEnd, seg.Span.Begin)
		switch {
		case cmp < 0:
			// there is a gap from the end of the last segment, and the start of this one.
			// we need to copy from the root first.
			span, err := gotfs.NewSpan(gotkv.Span{Begin: lastEnd, End: seg.Span.Begin})
			if err != nil {
				return err
			}
			if err := b.CopyFrom(ctx, root.ToGotKV(), span); err != nil {
				return err
			}
		case cmp == 0:
			// don't need to copy anything from old root
		case cmp > 0:
			return fmt.Errorf("out of order segments lastEnd=%v seg=%v", lastEnd, seg)
		}
		span, err := gotfs.NewSpan(seg.Span)
		if err != nil {
			return err
		}
		// now copy the segment from the diff.
		if err := b.CopyFrom(ctx, seg.Contents, span); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return Root{}, err
	}
	span, err := gotfs.NewSpan(gotkv.Span{Begin: lastEnd})
	if err != nil {
		return Root{}, err
	}
	// now copy from the root until infinity.
	if err := b.CopyFrom(ctx, root.ToGotKV(), span); err != nil {
		return Root{}, err
	}
	return b.Finish()
}

// Applied represents a sequence of Deltas applied to a base.
type Applied struct {
	s      stores.RO
	base   gotfs.Root
	deltas []Delta
}

func NewApplied(s stores.RO, base gotfs.Root, deltas []Delta) Applied {
	return Applied{s: s, base: base, deltas: deltas}
}

func (a *Applied) lower() *gotkvdelta.Applied {
	return &gotkvdelta.Applied{
		Base: a.base.ToGotKV(),
		Deltas: slices2.Map(a.deltas, func(x Delta) gotkvdelta.Delta {
			return gotkvdelta.Delta(x)
		}),
	}
}

func (a *Applied) Iterate(span gotfs.Span) gotkvdelta.Iterator {
	return a.lower().Iterate(span.ToSpan())
}
