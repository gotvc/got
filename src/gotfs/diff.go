package gotfs

import (
	"context"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

type Differ struct {
	diff *gotkv.Differ

	dent gotkv.DEntry
}

func (mach *Machine) NewDiffer(ms stores.Reading, left, right Root) *Differ {
	return &Differ{
		diff: mach.gotkv.NewDiffer(ms, left.ToGotKV(), right.ToGotKV(), gotkv.TotalSpan()),
	}
}

func (d *Differ) Next(ctx context.Context, dsts []DeltaEntry) (int, error) {
	dst := &dsts[0]
	*dst = DeltaEntry{}
	for {
		if err := streams.NextUnit(ctx, d.diff, &d.dent); err != nil {
			return 0, err
		}
		var key Key
		if err := key.Unmarshal(d.dent.Key); err != nil {
			return 0, err
		}
		// delete info
		if key.IsInfo() {
			dst.Path = key.Path()
			if !d.dent.Right.Ok {
				dst.Delete = &struct{}{}
			} else {
				info, err := parseInfo(d.dent.Right.X)
				if err != nil {
					return 0, err
				}
				dst.PutInfo = info
			}
			d.seekPast(ctx, key.Path())
			return 1, nil
		} else {
			if err := unmarshalExtentKey(d.dent.Key, &key); err != nil {
				return 0, err
			}
			p := key.Path()
			endAt := key.EndAt()
			if dst.Path == "" {
				dst.Path = p
				dst.PutContent = &PutContent{Begin: endAt}
			} else if dst.Path != p {
				return 1, nil
			}
			dst.PutContent.End = endAt
			if d.dent.Right.Ok {
				ext, err := parseExtent(d.dent.Right.X)
				if err != nil {
					return 0, err
				}
				dst.PutContent.Begin = min(dst.PutContent.Begin, endAt-uint64(ext.Length))
				dst.PutContent.Extents = append(dst.PutContent.Extents, *ext)
			} else if endAt == dst.PutContent.Begin {
				endAt = 0
			}
		}
	}
}

func (d *Differ) seekPast(ctx context.Context, p string) {
	prefix := newInfoKey(p).Prefix(nil)
	if err := d.diff.Seek(ctx, gotkv.PrefixEnd(prefix)); err != nil && !streams.IsEOS(err) {
		logctx.Error(ctx, "seeking", zap.Error(err))
	}
}
