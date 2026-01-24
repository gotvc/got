package gotfs

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/gotvc/got/src/gotfs/gotlob"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
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

func (mach *Machine) NewDeltaIterator(ms, ds stores.Reading, delta Delta) *DeltaIterator {
	return &DeltaIterator{
		iter: mach.gotkv.NewIterator(ms, gotkv.Root(delta), gotkv.TotalSpan()),
	}
}

func (di *DeltaIterator) Next(ctx context.Context, dsts []DeltaEntry) (int, error) {
	dst := &dsts[0]
	*dst = DeltaEntry{}
	for {
		if err := di.iter.Peek(ctx, &di.ent); err != nil {
			if streams.IsEOS(err) && dst.PutContent != nil {
				return 1, nil
			}
			return 0, err
		}
		var key Key
		if err := key.Unmarshal(di.ent.Key); err != nil {
			return 0, err
		}
		if key.IsInfo() {
			dst.Path = key.Path()
			if len(di.ent.Value) == 0 {
				dst.Delete = &struct{}{}
			} else {
				info, err := parseInfo(di.ent.Value)
				if err != nil {
					return 0, err
				}
				dst.PutInfo = info
			}
			if err := streams.NextUnit(ctx, di.iter, &di.ent); err != nil {
				return 0, err
			}
			return 1, nil
		} else {
			if err := unmarshalExtentKey(di.ent.Key, &key); err != nil {
				return 0, err
			}
			p := key.Path()
			offset := key.EndAt()
			if dst.Path == "" {
				dst.Path = p
				dst.PutContent = &PutContent{
					Begin: 0,
					End:   offset,
				}
			} else if dst.Path != p {
				return 0, nil
			}
			dst.PutContent.End = offset
			if len(di.ent.Value) > 0 {
				ext, err := parseExtent(di.ent.Value)
				if err != nil {
					return 0, err
				}
				dst.PutContent.Extents = append(dst.PutContent.Extents, *ext)
			}
			if err := streams.NextUnit(ctx, di.iter, &di.ent); err != nil {
				return 0, err
			}
			return 1, nil
		}
	}
}

func (mach *Machine) NewDeltaBuilder(ms, ds stores.RW) *DeltaBuilder {
	return &DeltaBuilder{
		b: mach.gotkv.NewBuilder(ms),
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
		return db.b.Put(ctx, newInfoKey(x.Path).Marshal(nil), nil)
	case x.PutInfo != nil:
		return db.b.Put(ctx, newInfoKey(x.Path).Marshal(nil), x.PutInfo.marshal())
	case x.PutContent != nil:
		k := newInfoKey(x.Path).Prefix(nil)
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
