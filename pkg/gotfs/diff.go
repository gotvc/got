package gotfs

import (
	"context"
	"errors"

	"github.com/brendoncarroll/go-exp/streams"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/gotvc/got/pkg/gotkv"
	"go.uber.org/zap"
)

type Differ struct {
	diff *gotkv.Differ

	dent gotkv.DEntry
}

func (ag *Agent) NewDiffer(ms cadata.Store, left, right Root) *Differ {
	return &Differ{
		diff: ag.gotkv.NewDiffer(ms, left.ToGotKV(), right.ToGotKV(), gotkv.TotalSpan()),
	}
}

func (d *Differ) Next(ctx context.Context, dst *DeltaEntry) error {
	*dst = DeltaEntry{}
	for {
		if err := d.diff.Next(ctx, &d.dent); err != nil {
			return err
		}
		switch {
		// delete info
		case isInfoKey(d.dent.Key):
			p, err := parseInfoKey(d.dent.Key)
			if err != nil {
				return err
			}
			dst.Path = p
			if !d.dent.Right.Ok {
				dst.Delete = &struct{}{}
			} else {
				info, err := parseInfo(d.dent.Right.X)
				if err != nil {
					return err
				}
				dst.PutInfo = info
			}
			d.seekPast(ctx, p)
			return nil

		case isExtentKey(d.dent.Key):
			p, offset, err := splitExtentKey(d.dent.Key)
			if err != nil {
				return err
			}
			if dst.Path == "" {
				dst.Path = p
				dst.PutContent = &PutContent{Begin: offset}
			} else if dst.Path != p {
				return nil
			}
			dst.PutContent.End = offset
			if d.dent.Right.Ok {
				ext, err := parseExtent(d.dent.Right.X)
				if err != nil {
					return err
				}
				dst.PutContent.Begin = min(dst.PutContent.Begin, offset-uint64(ext.Length))
				dst.PutContent.Extents = append(dst.PutContent.Extents, *ext)
			} else if offset == dst.PutContent.Begin {
				offset = 0
			}
		default:
			return errors.New("unrecognized key")
		}
	}
}

func (d *Differ) seekPast(ctx context.Context, p string) {
	if err := d.diff.Seek(ctx, gotkv.PrefixEnd(makeInfoKey(p))); err != nil && !streams.IsEOS(err) {
		logctx.Error(ctx, "seeking", zap.Error(err))
	}
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
