package gotfs

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

func Dump(ctx context.Context, s stores.Reading, root Root, w io.Writer) error {
	bw := bufio.NewWriter(w)
	ag := NewMachine()
	it := ag.gotkv.NewIterator(s, *root.toGotKV(), gotkv.TotalSpan())
	var ent gotkv.Entry
	for err := streams.NextUnit(ctx, it, &ent); !streams.IsEOS(err); err = streams.NextUnit(ctx, it, &ent) {
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
			fmt.Fprintf(bw, "METADATA\t%q\tmode=%o,attrs=%v\n", ent.Key, md.Mode, md.Attrs)
		}
	}
	return bw.Flush()
}

// Equal returns true if a and b contain equivalent data.
func Equal(a, b Root) bool {
	return gdat.Equal(a.Ref, b.Ref) && a.Depth == b.Depth
}
