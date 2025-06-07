package gotfs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gotvc/got/pkg/gotfs/gotlob"
	"github.com/gotvc/got/pkg/gotkv"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/cadata"
)

// Delta is the different between two Roots
type Delta gotkv.Root

type DeltaEntry struct {
	Path string

	PutInfo    *Info
	Delete     *struct{}
	PutContent *PutContent
}

type PutContent struct {
	Begin, End uint64
	Extents    []Extent
}

var (
	_ streams.Iterator[DeltaEntry] = &DeltaIterator{}
	_ streams.Iterator[DeltaEntry] = &Differ{}
)

type DeltaIterator struct {
	iter *gotkv.Iterator

	ent gotkv.Entry
}

func (ag *Agent) NewDeltaIterator(ms, ds cadata.Store, delta Delta) *DeltaIterator {
	return &DeltaIterator{
		iter: ag.gotkv.NewIterator(ms, gotkv.Root(delta), gotkv.TotalSpan()),
	}
}

func (di *DeltaIterator) Next(ctx context.Context, dst *DeltaEntry) error {
	*dst = DeltaEntry{}
	for {
		if err := di.iter.Peek(ctx, &di.ent); err != nil {
			if streams.IsEOS(err) && dst.PutContent != nil {
				return nil
			}
			return err
		}
		switch {
		case isInfoKey(di.ent.Key):
			p, err := parseInfoKey(di.ent.Key)
			if err != nil {
				return err
			}
			dst.Path = p
			if len(di.ent.Value) == 0 {
				dst.Delete = &struct{}{}
			} else {
				info, err := parseInfo(di.ent.Value)
				if err != nil {
					return err
				}
				dst.PutInfo = info
			}
			return di.iter.Next(ctx, &di.ent)
		case isExtentKey(di.ent.Key):
			p, offset, err := splitExtentKey(di.ent.Key)
			if err != nil {
				return err
			}
			if dst.Path == "" {
				dst.Path = p
				dst.PutContent = &PutContent{
					Begin: 0,
					End:   offset,
				}
			} else if dst.Path != p {
				return nil
			}
			dst.PutContent.End = offset
			if len(di.ent.Value) > 0 {
				ext, err := parseExtent(di.ent.Value)
				if err != nil {
					return err
				}
				dst.PutContent.Extents = append(dst.PutContent.Extents, *ext)
			}
			return di.iter.Next(ctx, &di.ent)
		default:
			return errors.New("unrecognized key")
		}
	}
}

func (ag *Agent) NewDeltaBuilder(ms, ds cadata.Store) *DeltaBuilder {
	return &DeltaBuilder{
		b: ag.gotkv.NewBuilder(ms),
	}
}

type DeltaBuilder struct {
	b *gotkv.Builder
}

func (db *DeltaBuilder) Delete(ctx context.Context, p string) error {
	return db.write(ctx, DeltaEntry{
		Path:   p,
		Delete: &struct{}{},
	})
}

func (db *DeltaBuilder) PutInfo(ctx context.Context, p string, info *Info) error {
	return db.write(ctx, DeltaEntry{
		Path:    p,
		PutInfo: info,
	})
}

func (db *DeltaBuilder) Finish(ctx context.Context) (*Delta, error) {
	root, err := db.b.Finish(ctx)
	if err != nil {
		return nil, err
	}
	return (*Delta)(root), nil
}

func (db *DeltaBuilder) write(ctx context.Context, x DeltaEntry) error {
	switch {
	case x.Delete != nil:
		return db.b.Put(ctx, makeInfoKey(x.Path), nil)
	case x.PutInfo != nil:
		x.PutInfo.Nonempty = true
		return db.b.Put(ctx, makeInfoKey(x.Path), x.PutInfo.marshal())
	case x.PutContent != nil:
		k := makeExtentPrefix(x.Path)
		k = binary.BigEndian.AppendUint64(k, x.PutContent.End)
		if len(x.PutContent.Extents) == 0 {
			return db.b.Put(ctx, k, nil)
		}
		for _, ext := range x.PutContent.Extents {
			if err := db.b.Put(ctx, k, gotlob.MarshalExtent(&ext)); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("can't marshal %+v", x)
	}
}
