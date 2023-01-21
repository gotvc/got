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

func DebugTree(ctx context.Context, cmp CompareFunc, dec Decoder, cs cadata.Store, x Root, w io.Writer) error {
	s := wrapStore(cs)
	bw, ok := w.(*bufio.Writer)
	if !ok {
		bw = bufio.NewWriter(w)
	}
	max := x.Depth
	var debugTree func(Root)
	debugTree = func(x Root) {
		indent := ""
		for i := 0; i < int(max-x.Depth); i++ {
			indent += "  "
		}
		ctx := context.TODO()
		sr := NewStreamReader(StreamReaderParams{
			Store:   s,
			Compare: cmp,
			Decoder: dec,
			Indexes: []Index{{Ref: x.Ref, First: x.First}},
		})
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

func wrapStore(s cadata.Store) Store {
	op := gdat.NewOperator()
	return &storeWrapper{s: s, op: &op}
}

type storeWrapper struct {
	s  cadata.Store
	op *gdat.Operator
}

func (sw *storeWrapper) Post(ctx context.Context, data []byte) (Ref, error) {
	ref, err := sw.op.Post(ctx, sw.s, data)
	if err != nil {
		return Ref{}, err
	}
	return *ref, nil
}

func (sw *storeWrapper) Get(ctx context.Context, ref Ref, buf []byte) (int, error) {
	return sw.op.Read(ctx, sw.s, ref, buf)
}

func (sw *storeWrapper) MaxSize() int {
	return sw.s.MaxSize()
}
