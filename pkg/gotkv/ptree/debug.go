package ptree

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

func DebugTree[Ref any](ctx context.Context, params ReadParams[Ref], x Root[Ref], w io.Writer) error {
	bw, ok := w.(*bufio.Writer)
	if !ok {
		bw = bufio.NewWriter(w)
	}
	max := x.Depth
	var debugTree func(Root[Ref])
	debugTree = func(x Root[Ref]) {
		indent := ""
		for i := 0; i < int(max-x.Depth); i++ {
			indent += "  "
		}
		ctx := context.TODO()
		sr := NewStreamReader(StreamReaderParams[Ref]{
			Store:   params.Store,
			Compare: params.Compare,
			Decoder: params.NewDecoder(),
			Indexes: []Index[Ref]{{Ref: x.Ref, First: x.First}},
		})
		fmt.Fprintf(bw, "%sTREE NODE: %v %d\n", indent, x.Ref, x.Depth)
		if x.Depth == 0 {
			for {
				var ent Entry
				if err := sr.Next(ctx, &ent); err != nil {
					if err == kvstreams.EOS {
						break
					}
					panic(err)
				}
				fmt.Fprintf(w, "%s ENTRY key=%q value=%q\n", indent, string(ent.Key), string(ent.Value))
			}
		} else {
			for {
				var ent Entry
				if err := sr.Next(ctx, &ent); err != nil {
					if err == kvstreams.EOS {
						break
					}
					panic(err)
				}
				ref, err := params.ParseRef(ent.Value)
				if err != nil {
					panic(err)
				}
				fmt.Fprintf(bw, "%s INDEX first=%q -> ref=%v\n", indent, string(ent.Key), ref)
				debugTree(Root[Ref]{Ref: ref, First: ent.Key, Depth: x.Depth - 1})
			}
		}
	}
	debugTree(x)
	return bw.Flush()
}
