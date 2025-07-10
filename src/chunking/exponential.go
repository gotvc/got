package chunking

import (
	"bytes"
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

func (e *Exponential) Write(data []byte) (int, error) {
	var n int
	for n < len(data) {
		spaceLeft := e.targetSize() - e.buf.Len()
		end := n + spaceLeft
		if len(data) < end {
			end = len(data)
		}
		n2, err := e.buf.Write(data[n:end])
		n += n2
		if err != nil {
			return n, err
		}
		if e.buf.Len() == e.targetSize() {
			if err := e.emit(); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

func (e *Exponential) Buffered() int {
	return e.buf.Len()
}

func (e *Exponential) Flush() error {
	return e.emit()
}

func (e *Exponential) Reset() {
	e.buf.Reset()
	e.count = 0
}

func (e *Exponential) targetSize() int {
	size := uint64(e.minSize)
	shift := uint64(e.count / e.period)
	if shift >= 32 {
		return e.maxSize
	}
	size <<= shift
	if size > uint64(e.maxSize) {
		size = uint64(e.maxSize)
	}
	return int(size)
}

func (e *Exponential) emit() error {
	defer e.buf.Reset()
	if e.buf.Len() > 0 {
		e.count++
		return e.fn(e.buf.Bytes())
	}
	return nil
}
