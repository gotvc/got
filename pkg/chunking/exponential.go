package chunking

import (
	"bytes"

	"github.com/pkg/errors"
)

type Exponential struct {
	minSize, maxSize, period int
	count                    int
	buf                      *bytes.Buffer
	fn                       ChunkHandler
}

func NewExponential(minSize, maxSize, period int, fn ChunkHandler) *Exponential {
	return &Exponential{
		minSize: minSize,
		maxSize: maxSize,
		period:  period,
		buf:     bytes.NewBuffer(nil),
		fn:      fn,
	}
}

func (e *Exponential) Write(p []byte) (int, error) {
	if e.buf.Len()+len(p) < e.targetSize() {
		return e.buf.Write(p)
	}
	x := e.targetSize() - e.buf.Len()
	if _, err := e.buf.Write(p[:x]); err != nil {
		return 0, err
	}
	if err := e.emit(); err != nil {
		return x, err
	}
	return e.Write(p[x:])
}

func (e *Exponential) WriteNoSplit(p []byte) (int, error) {
	return 0, errors.Errorf("Exponential does not support WriteNoSplit")
}

func (e *Exponential) Buffered() int {
	return e.buf.Len()
}

func (e *Exponential) Flush() error {
	return e.emit()
}

func (e *Exponential) targetSize() int {
	size := e.minSize
	size <<= (e.count / e.period)
	if size > e.maxSize {
		size = e.maxSize
	}
	return size
}

func (e *Exponential) emit() error {
	defer e.buf.Reset()
	if e.buf.Len() > 0 {
		e.count++
		return e.fn(e.buf.Bytes())
	}
	return nil
}
