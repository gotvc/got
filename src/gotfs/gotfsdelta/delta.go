// Package gotfsdelta implements the Delta format
package gotfsdelta

import (
	"context"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/maybe"
	"go.brendoncarroll.net/exp/streams"
)

type Machine struct {
	fs *gotfs.Machine
	kv *gotkv.Machine
}

type Delta gotkv.Delta

type DeltaWriter struct {
	s stores.WO

	// prefix is the minimum prefix before the current edit must be closed.
	minPrefix string
	lastKey   maybe.Maybe[gotfs.Key]
	kvw       gotkv.DeltaWriter
}

func (m *Machine) NewDeltaWriter(s stores.RW) DeltaWriter {
	return DeltaWriter{s: s, kvw: m.kv.NewDeltaWriter(s)}
}

func (dw *DeltaWriter) Finish(ctx context.Context) (Delta, error) {
	d, err := dw.kvw.Finish(ctx)
	if err != nil {
		return Delta{}, err
	}
	return Delta(d), nil
}

func (dw *DeltaWriter) Put(ctx context.Context, k gotfs.Key, val gotfs.Value) error {
	if dw.lastKey.Ok && dw.lastKey.X.Path() != k.Path() {
		// next path, close the last edit.
		if err := dw.kvw.EndEdit(ctx, dw.lastKey.X.Marshal(nil)); err != nil {
			return err
		}
		if err := dw.kvw.BeginEdit(ctx, k.Marshal(nil)); err != nil {
			return err
		}
	} else if !dw.lastKey.Ok {
		if err := dw.kvw.BeginEdit(ctx, k.Marshal(nil)); err != nil {
			return err
		}
	}
	if err := dw.kvw.Put(ctx, k.Marshal(nil), val.Marshal(nil)); err != nil {
		return err
	}
	dw.haveKey = true
	dw.lastKey = k
	return nil
}

func (dw *DeltaWriter) Info(ctx context.Context, next gotfs.Info) error {
	return nil
}

// ReadFromDiffer reads all the entries from the differ stream and adds them to the delta.
func (dw *DeltaWriter) ReadFromDiffer(ctx context.Context, dfr *gotfs.Differ) error {
	return streams.ForEach(ctx, dfr, func(dent gotfs.DiffEntry) error {
		rok := dent.Right.Ok
		lok := dent.Left.Ok
		if dent.Key.IsInfo() {
			linfo := dent.Left.X.Info
			rinfo := dent.Right.X.Info
			switch {
			case dent.IsCreate():
				return dw.Info(ctx, dent.Key, rinfo.Mode)
			case dent.IsDelete():
				return dw.Info(ctx, linfo)
			default:
				// metadata change
				if rok {

				} else {

				}
			}
		} else {

		}
		return nil
	})
}
