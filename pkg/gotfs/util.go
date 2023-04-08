package gotfs

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
)

// ChangesOnBase inserts segments from base between each Segment in changes.
func ChangesOnBase(base Root, changes []Segment) []Segment {
	var segs []Segment
	for i := range changes {
		// create the span to reference the root, should be inbetween the two entries from segs
		var baseSpan gotkv.Span
		if i > 0 {
			baseSpan.Begin = segs[len(segs)-1].Span.End
		}
		baseSpan.End = changes[i].Span.Begin
		baseSeg := Segment{Span: baseSpan, Contents: Expr{Root: base}}

		segs = append(segs, baseSeg)
		segs = append(segs, changes[i])
	}
	if len(segs) > 0 {
		segs = append(segs, Segment{
			Span: gotkv.Span{
				Begin: segs[len(segs)-1].Span.End,
				End:   nil,
			},
			Contents: Expr{Root: base},
		})
	}
	return segs
}

func Dump(ctx context.Context, s Store, root Root, w io.Writer) error {
	bw := bufio.NewWriter(w)
	op := NewOperator()
	it := op.gotkv.NewIterator(s, *root.toGotKV(), gotkv.TotalSpan())
	var ent gotkv.Entry
	for err := it.Next(ctx, &ent); err != gotkv.EOS; err = it.Next(ctx, &ent) {
		if err != nil {
			return err
		}
		switch {
		case isExtentKey(ent.Key):
			ext, err := parseExtent(ent.Value)
			if err != nil {
				fmt.Fprintf(bw, "EXTENT (INVALID):\t%q\t%q\n", ent.Key, ent.Value)
				continue
			}
			fmt.Fprintf(bw, "EXTENT\t%q\toffset=%d,length=%d,ref=%s\n", ent.Key, ext.Offset, ext.Length, ext.Ref)
		default:
			md, err := parseInfo(ent.Value)
			if err != nil {
				fmt.Fprintf(bw, "METADATA (INVALID):\t%q\t%q\n", ent.Key, ent.Value)
				continue
			}
			fmt.Fprintf(bw, "METADATA\t%q\tmode=%o,labels=%v\n", ent.Key, md.Mode, md.Labels)
		}
	}
	return bw.Flush()
}

// Equal returns true if a and b contain equivalent data.
func Equal(a, b Root) bool {
	return gdat.Equal(a.Ref, b.Ref) && a.Depth == b.Depth
}
