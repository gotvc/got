package ptree

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

func DebugTree(ctx context.Context, s cadata.Store, x Root, w io.Writer) error {
	bw, ok := w.(*bufio.Writer)
	if !ok {
		bw = bufio.NewWriter(w)
	}
	max := x.Depth
	op := gdat.NewOperator()
	var debugTree func(Root)
	debugTree = func(x Root) {
		indent := ""
		for i := 0; i < int(max-x.Depth); i++ {
			indent += "  "
		}
		ctx := context.TODO()
		sr := NewStreamReader(s, &op, []Index{{Ref: x.Ref, First: x.First}})
		fmt.Fprintf(bw, "%sTREE NODE: %s %d\n", indent, x.Ref.CID.String(), x.Depth)
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
				ref, err := gdat.ParseRef(ent.Value)
				if err != nil {
					panic(err)
				}
				fmt.Fprintf(bw, "%s INDEX first=%q -> ref=%s\n", indent, string(ent.Key), ref.CID.String())
				debugTree(Root{Ref: *ref, First: ent.Key, Depth: x.Depth - 1})
			}
		}
	}
	debugTree(x)
	return bw.Flush()
}
