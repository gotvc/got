package gotfs

import (
	"context"

	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/streams"
)

type Delta gotkv.Delta

// DeltaWriter writes a stream of GotFS filesystem edits to a store
type DeltaWriter struct {
	s stores.WO

	kvw        gotkv.DeltaWriter
	differPath string
}

func (m *Machine) NewDeltaWriter(s stores.RW) DeltaWriter {
	return DeltaWriter{
		s:   s,
		kvw: m.gotkv.NewDeltaWriter(s),
	}
}

func (dw *DeltaWriter) Finish(ctx context.Context) (Delta, error) {
	if err := dw.closeExtentEdit(ctx); err != nil {
		return Delta{}, err
	}
	d, err := dw.kvw.Finish(ctx)
	if err != nil {
		return Delta{}, err
	}
	return Delta(d), nil
}

// PutAllFileData replaces the file data extents for path p.
func (dw *DeltaWriter) PutAllFileData(ctx context.Context, p string, exts []Extent) error {
	ik := newInfoKey(p)
	prefix := ik.Prefix(nil)
	begin := infoKeyNext(ik.Marshal(nil))
	end := gotkv.PrefixEnd(prefix)

	if err := dw.kvw.BeginEdit(ctx, begin); err != nil {
		return err
	}
	var cum uint64
	for _, ext := range exts {
		cum += uint64(ext.Length)
		ek := NewExtentKey(p, cum)
		v := Value{Extent: ext}
		if err := dw.kvw.Put(ctx, ek.Marshal(nil), v.Marshal(false, nil)); err != nil {
			return err
		}
	}
	return dw.kvw.EndEdit(ctx, end)
}

// PutInfo writes a metadata entry for path p.
// An Info change is a single-item edit.
func (dw *DeltaWriter) PutInfo(ctx context.Context, p string, info Info) error {
	ik := newInfoKey(p)
	keyBytes := ik.Marshal(nil)
	endExcl := infoKeyNext(keyBytes)

	v := Value{Info: info}
	valBytes := v.Marshal(true, nil)

	if err := dw.kvw.BeginEdit(ctx, keyBytes); err != nil {
		return err
	}
	if err := dw.kvw.Put(ctx, keyBytes, valBytes); err != nil {
		return err
	}
	return dw.kvw.EndEdit(ctx, endExcl)
}

// DeletePath deletes the info at p, any extents if it is a regular file
// and all of the child directories if it is a directory.
func (dw *DeltaWriter) DeletePath(ctx context.Context, p string) error {
	ik := newInfoKey(p)
	span := ik.ChildrenSpan()

	if err := dw.kvw.BeginEdit(ctx, span.Begin); err != nil {
		return err
	}
	if err := dw.kvw.DeleteUntil(ctx, span.End); err != nil {
		return err
	}
	return dw.kvw.EndEdit(ctx, span.End)
}

// ReadFromDiffer reads all the entries from the differ stream and adds them to the delta.
func (dw *DeltaWriter) ReadFromDiffer(ctx context.Context, dfr *Differ) error {
	return streams.ForEach(ctx, dfr, func(dent DiffEntry) error {
		p := dent.Key.Path()
		rok := dent.Right.Ok
		if dent.Key.IsInfo() {
			if err := dw.closeExtentEdit(ctx); err != nil {
				return err
			}
			switch {
			case dent.IsCreate():
				return dw.PutInfo(ctx, p, dent.Right.X.Info)
			case dent.IsDelete():
				return dw.DeletePath(ctx, p)
			default:
				return dw.PutInfo(ctx, p, dent.Right.X.Info)
			}
		}
		if !rok {
			return nil
		}
		if dw.differPath != p {
			if err := dw.closeExtentEdit(ctx); err != nil {
				return err
			}
			ik := newInfoKey(p)
			begin := infoKeyNext(ik.Marshal(nil))
			if err := dw.kvw.BeginEdit(ctx, begin); err != nil {
				return err
			}
			dw.differPath = p
		}
		v := Value{Extent: dent.Right.X.Extent}
		return dw.kvw.Put(ctx, dent.Key.Marshal(nil), v.Marshal(false, nil))
	})
}

func (dw *DeltaWriter) closeExtentEdit(ctx context.Context) error {
	if dw.differPath == "" {
		return nil
	}
	p := dw.differPath
	dw.differPath = ""
	ik := newInfoKey(p)
	end := gotkv.PrefixEnd(ik.Prefix(nil))
	return dw.kvw.EndEdit(ctx, end)
}

func infoKeyNext(infoKey []byte) []byte {
	out := make([]byte, len(infoKey))
	copy(out, infoKey)
	out[len(out)-1] = 1
	return out
}
