package gotfs

import "github.com/gotvc/got/pkg/gotkv"

// ChangesOnBase inserts segments from base between each Segment in changes.
func ChangesOnBase(base Root, changes []Segment) []Segment {
	var segs []Segment
	for i := range changes {
		// create the span to reference the root, should be inbetween the two entries from segs
		var baseSpan gotkv.Span
		if i > 0 {
			baseSpan.Start = segs[len(segs)-1].Span.End
		}
		baseSpan.End = changes[i].Span.Start
		baseSeg := Segment{Root: base, Span: baseSpan}

		segs = append(segs, baseSeg)
		segs = append(segs, changes[i])
	}
	if len(segs) > 0 {
		segs = append(segs, Segment{
			Root: base,
			Span: gotkv.Span{
				Start: segs[len(segs)-1].Span.End,
				End:   nil,
			},
		})
	}
	return segs
}
