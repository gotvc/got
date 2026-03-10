package gotfs

import (
	"context"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/maybe"
	"go.brendoncarroll.net/exp/streams"
)

// DiffEntry is a single element of the difference between 2 filesystems.
type DiffEntry struct {
	Key Key

	Left  maybe.Maybe[Value]
	Right maybe.Maybe[Value]
}

// Differ iterates over the difference between 2 filesystems.
type Differ struct {
	kvdiff *gotkv.Differ

	dent gotkv.DEntry
}

func (mach *Machine) NewDiffer(ms stores.Reading, left, right Root) *Differ {
	span := gotkv.TotalSpan()
	return &Differ{
		kvdiff: mach.gotkv.NewDiffer(ms, left.ToGotKV(), right.ToGotKV(), span),
	}
}

func (d *Differ) Next(ctx context.Context, dsts []DiffEntry) (int, error) {
	dst := &dsts[0]
	dst.Left.Ok = false
	dst.Right.Ok = false
	if err := streams.NextUnit(ctx, d.kvdiff, &d.dent); err != nil {
		return 0, err
	}
	if err := dst.Key.Unmarshal(d.dent.Key); err != nil {
		return 0, err
	}
	if d.dent.Left.Ok {
		if err := dst.Left.X.unmarshal(dst.Key.IsInfo(), d.dent.Left.X); err != nil {
			return 0, err
		}
		dst.Left.Ok = true
	}
	if d.dent.Right.Ok {
		if err := dst.Right.X.unmarshal(dst.Key.IsInfo(), d.dent.Right.X); err != nil {
			return 0, err
		}
		dst.Right.Ok = true
	}
	return 1, nil
}

type InfoDiff struct {
	Path  string
	Left  maybe.Maybe[Info]
	Right maybe.Maybe[Info]
}

type InfoDiffer struct {
	inner *Differ
}

func (it *InfoDiffer) Next(ctx context.Context, dst []InfoDiff) (int, error) {
	for {
		de, err := streams.Next(ctx, it.inner)
		if err != nil {
			return 0, err
		}
		if !de.Key.IsInfo() {
			continue
		}
		mf := func(x Value) Info { return x.Info }
		dst[0].Path = de.Key.Path()
		dst[0].Left = maybe.Map(de.Left, mf)
		dst[0].Right = maybe.Map(de.Right, mf)
		return 1, nil
	}
}

func (mach *Machine) NewInfoDiffer(s stores.Reading, left, right Root) InfoDiffer {
	return InfoDiffer{
		inner: mach.NewDiffer(s, left, right),
	}
}
