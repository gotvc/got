package gotfs

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"runtime"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/got/pkg/chunking"
	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/brendoncarroll/got/pkg/stores"
	"github.com/pkg/errors"
)

const (
	minPartSize            = 1 << 12
	partSizeDoublingPeriod = 2
)

type writer struct {
	onPart  func(part *Part) error
	chunker *chunking.Exponential
	ctx     context.Context
}

func (o *Operator) newWriter(ctx context.Context, s cadata.Store, onPart func(*Part) error) *writer {
	if s.MaxSize() < minPartSize {
		panic(fmt.Sprint("store size too small", s.MaxSize()))
	}
	w := &writer{
		onPart: onPart,
		ctx:    ctx,
	}
	w.chunker = chunking.NewExponential(minPartSize, s.MaxSize(), partSizeDoublingPeriod, func(data []byte) error {
		ref, err := o.dop.Post(ctx, s, data)
		if err != nil {
			return err
		}
		part := &Part{
			Offset: 0,
			Length: uint32(len(data)),
			Ref:    gdat.MarshalRef(*ref),
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

// CreateFileRoot creates a new filesystem with the contents read from r at the root
func (o *Operator) CreateFileRoot(ctx context.Context, ms, ds Store, r io.Reader) (*Root, error) {
	ams := stores.NewAsyncStore(ms, runtime.GOMAXPROCS(0))
	ads := stores.NewAsyncStore(ds, runtime.GOMAXPROCS(0))
	b := o.gotkv.NewBuilder(ams)

	// metadata entry
	md := Metadata{
		Mode: 0o644,
	}
	if err := b.Put(ctx, []byte{}, md.marshal()); err != nil {
		return nil, err
	}
	// content
	var total uint64
	w := o.newWriter(ctx, ads, func(part *Part) error {
		total += uint64(part.Length)
		key := makePartKey("", total)
		return b.Put(ctx, key, part.marshal())
	})
	if _, err := io.Copy(w, r); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	root, err := b.Finish(ctx)
	if err != nil {
		return nil, err
	}
	if err := ads.Close(); err != nil {
		return nil, err
	}
	if err := ams.Close(); err != nil {
		return nil, err
	}
	return root, nil
}

// CreateFile creates a file at p with data from r
// If there is an entry at p CreateFile returns an error
// ms is the store used for metadata
// ds is the store used for data.
func (o *Operator) CreateFile(ctx context.Context, ms, ds Store, x Root, p string, r io.Reader) (*Root, error) {
	p = cleanPath(p)
	if err := o.checkNoEntry(ctx, ms, x, p); err != nil {
		return nil, err
	}
	fileRoot, err := o.CreateFileRoot(ctx, ms, ds, r)
	if err != nil {
		return nil, err
	}
	return o.Graft(ctx, ms, x, p, *fileRoot)
}

// SizeOfFile returns the size of the file at p in bytes.
func (o *Operator) SizeOfFile(ctx context.Context, s Store, x Root, p string) (uint64, error) {
	p = cleanPath(p)
	gotkv := gotkv.NewOperator()
	under := append([]byte(p), 0x01)
	key, err := gotkv.MaxKey(ctx, s, x, under)
	if err != nil {
		return 0, err
	}
	// offset of key
	if len(key) < 8 {
		return 0, errors.Errorf("key too short")
	}
	offset := binary.BigEndian.Uint64(key[len(key)-8:])
	return offset, nil
}

// ReadFileAt fills `buf` with data in the file at `p` starting at offset `start`
func (o *Operator) ReadFileAt(ctx context.Context, ms, ds Store, x Root, p string, start uint64, buf []byte) (int, error) {
	p = cleanPath(p)
	_, err := o.GetFileMetadata(ctx, ms, x, p)
	if err != nil {
		return 0, err
	}
	key := makePartKey(p, start)
	span := gotkv.Span{
		Start: key,
		End:   fileSpanEnd(p),
	}
	it := o.gotkv.NewIterator(ms, x, span)
	var n int
	for n < len(buf) {
		n2, err := o.readFromIterator(ctx, it, ds, start, buf[n:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return n, err
		}
		n += n2
		start += uint64(n2)
	}
	if n > 0 {
		return n, nil
	}
	return n, io.EOF
}

func (o *Operator) readFromIterator(ctx context.Context, it gotkv.Iterator, ds cadata.Store, start uint64, buf []byte) (int, error) {
	ent, err := it.Next(ctx)
	if err != nil {
		return 0, err
	}
	_, extentEnd, err := splitPartKey(ent.Key)
	if err != nil {
		return 0, err
	}
	if extentEnd <= start {
		return 0, nil
	}
	part, err := parsePart(ent.Value)
	if err != nil {
		return 0, err
	}
	ref, err := gdat.ParseRef(part.Ref)
	if err != nil {
		return 0, err
	}
	extentStart := extentEnd - uint64(part.Length)
	if start < extentStart {
		return 0, errors.Errorf("incorrect part extentStart=%d asked for start=%d", extentEnd, start)
	}
	var n int
	if err := o.dop.GetF(ctx, ds, *ref, func(data []byte) error {
		if int(part.Offset) >= len(data) {
			return errors.Errorf("part offset %d is >= len(data) %d", part.Offset, len(data))
		}
		data = data[part.Offset : part.Offset+part.Length]
		n += copy(buf, data[start-extentStart:])
		return nil
	}); err != nil {
		return 0, err
	}
	return n, err
}

func (o *Operator) WriteFileAt(ctx context.Context, s Store, x Root, p string, start uint64, data []byte) (*Ref, error) {
	md, err := o.GetFileMetadata(ctx, s, x, p)
	if err != nil {
		return nil, err
	}
	panic(md)
}
