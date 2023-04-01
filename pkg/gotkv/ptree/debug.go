package ptree

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

func DebugTree[T, Ref any](ctx context.Context, params ReadParams[T, Ref], x Root[T, Ref], w io.Writer) error {
	bw, ok := w.(*bufio.Writer)
	if !ok {
		bw = bufio.NewWriter(w)
	}
	max := x.Depth
	var debugTree func(Root[T, Ref])
	debugTree = func(x Root[T, Ref]) {
		indent := ""
		for i := 0; i < int(max-x.Depth); i++ {
			indent += "  "
		}
		sr := NewStreamReader(StreamReaderParams[T, Ref]{
			Store:   params.Store,
			Compare: params.Compare,
			Decoder: params.NewDecoder(),
			Indexes: []Index[T, Ref]{{Ref: x.Ref, Span: x.Span}},
		})
		fmt.Fprintf(bw, "%sTREE NODE: %v %d\n", indent, x.Ref, x.Depth)
		if x.Depth == 0 {
			for {
				var ent T
				if err := sr.Next(ctx, &ent); err != nil {
					if err == kvstreams.EOS {
						break
					}
					panic(err)
				}
				fmt.Fprintf(w, "%s ENTRY %v\n", indent, ent)
			}
		} else {
			var ent T
			for {
				if err := sr.Next(ctx, &ent); err != nil {
					if err == kvstreams.EOS {
						break
					}
					panic(err)
				}
				idx, err := params.ConvertEntry(ent)
				if err != nil {
					panic(err)
				}
				fmt.Fprintf(bw, "%s INDEX span=%v -> ref=%v\n", indent, idx.Span, idx.Ref)
				debugTree(Root[T, Ref]{Ref: idx.Ref, Span: idx.Span, Depth: x.Depth - 1})
			}
		}
	}
	debugTree(x)
	return bw.Flush()
}
