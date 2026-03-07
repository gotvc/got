package gotfsvm

import (
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
)

// ChangesOnBase inserts segments from base between each Segment in changes.
func ChangesOnBase(base gotfs.Root, changes []gotfs.Segment) []gotfs.Segment {
	var segs []gotfs.Segment
	for i := range changes {
		// create the span to reference the root, should be inbetween the two entries from segs
		var baseSpan gotkv.Span
		if i > 0 {
			baseSpan.Begin = segs[len(segs)-1].Span.End
		}
		baseSpan.End = changes[i].Span.Begin
		baseSeg := gotfs.Segment{Span: baseSpan, Contents: base.ToGotKV()}

		segs = append(segs, baseSeg)
		segs = append(segs, changes[i])
	}
	if len(segs) > 0 {
		segs = append(segs, gotfs.Segment{
			Span: gotkv.Span{
				Begin: segs[len(segs)-1].Span.End,
				End:   nil,
			},
			Contents: base.ToGotKV(),
		})
	}
	return segs
}
