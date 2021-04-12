package gotfs

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/chunking"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/pkg/errors"
)

const (
	minPartSize            = 1 << 12
	maxPartSize            = 1 << 20
	partSizeDoublingPeriod = 1
)

type writer struct {
	onPart  func(part Part) error
	chunker *chunking.Exponential
	ctx     context.Context
}

func (o *Operator) newWriter(ctx context.Context, s cadata.Store, onPart func(Part) error) *writer {
	w := &writer{
		onPart: onPart,
		ctx:    ctx,
	}
	w.chunker = chunking.NewExponential(minPartSize, maxPartSize, partSizeDoublingPeriod, func(data []byte) error {
		ref, err := o.dop.Post(ctx, s, data)
		if err != nil {
			return err
		}
		part := Part{
			Offset: 0,
			Length: uint32(len(data)),
			Ref:    *ref,
		}
		return w.onPart(part)
	})
	return w
}

func (w *writer) Write(p []byte) (int, error) {
	return w.chunker.Write(p)
}

func (w *writer) Flush() error {
	return w.chunker.Flush()
}

// CreateFileFrom creates a file at p with data from r
func (o *Operator) CreateFileFrom(ctx context.Context, s Store, x Root, p string, r io.Reader) (*gotkv.Root, error) {
	if err := o.checkNoEntry(ctx, s, x, p); err != nil {
		return nil, err
	}
	// TODO: add this back after we have migrated to the builder.
	// as := cadata.NewAsyncStore(s, runtime.GOMAXPROCS(0))
	// s = as
	// create metadata entry
	md := Metadata{
		Mode: 0o644,
	}
	if x2, err := o.PutMetadata(ctx, s, x, p, md); err != nil {
		return nil, err
	} else {
		x = *x2
	}

	var total uint64
	w := o.newWriter(ctx, s, func(part Part) error {
		total += uint64(part.Length)
		key := makePartKey(p, total)
		if x2, err := o.gotkv.Put(ctx, s, x, key, part.marshal()); err != nil {
			return err
		} else {
			x = *x2
		}
		return nil
	})
	if _, err := io.Copy(w, r); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return &x, nil
}

func (o *Operator) SizeOfFile(ctx context.Context, s Store, x Root, p string) (int, error) {
	gotkv := gotkv.NewOperator()
	key, err := gotkv.MaxKey(ctx, s, x, []byte(p))
	if err != nil {
		return 0, err
	}
	// offset of key
	if len(key) < 8 {
		return 0, errors.Errorf("key too short")
	}
	offset := binary.BigEndian.Uint64(key[len(key)-8:])
	// size of part at that key
	var size int
	if err := gotkv.GetF(ctx, s, x, []byte(p), func(v []byte) error {
		size = len(v)
		return nil
	}); err != nil {
		return 0, err
	}
	return int(offset) + size, nil
}

func (o *Operator) ReadFileAt(ctx context.Context, s Store, x Root, p string, start uint64, buf []byte) (int, error) {
	kvop := gotkv.NewOperator()
	dop := gdat.NewOperator()
	_, err := o.GetFileMetadata(ctx, s, x, p)
	if err != nil {
		return 0, err
	}
	key := makePartKey(p, start)
	span := gotkv.Span{
		Start: key,
		End:   fileSpanEnd(p),
	}
	it := kvop.NewIterator(s, x, span)
	var n int
	for n < len(buf) {
		ent, err := it.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		_, extentEnd, err := splitPartKey(ent.Key)
		if err != nil {
			return 0, err
		}
		if extentEnd <= start {
			continue // this shouldn't happen
		}
		part, err := parsePart(ent.Value)
		if err != nil {
			return 0, err
		}
		extentStart := extentEnd - uint64(part.Length)
		if err := dop.GetF(ctx, s, part.Ref, func(data []byte) error {
			data = data[part.Offset : part.Offset+part.Length]
			n += copy(buf, data[start-extentStart:])
			return nil
		}); err != nil {
			return 0, err
		}
	}
	if n > 0 {
		return n, nil
	}
	return n, io.EOF
}

func (o *Operator) WriteFileAt(ctx context.Context, s Store, x Root, p string, start uint64, data []byte) (*Ref, error) {
	md, err := o.GetFileMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	panic(md)
}
