package ptree

import (
	"bytes"
	"math/bits"

	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/chmduquesne/rollinghash/buzhash64"
	"github.com/pkg/errors"
)

type Ref = gdat.Ref

type Chunker struct {
	log2AvgSize, maxSize int
	onChunk              func(data []byte) error
	rh                   *buzhash64.Buzhash64
	buf                  bytes.Buffer
}

func NewChunker(avgSize, maxSize int, onChunk func(data []byte) error) *Chunker {
	if bits.OnesCount(uint(avgSize)) != 1 {
		panic("avgSize must be power of 2")
	}
	log2AvgSize := bits.TrailingZeros64(uint64(avgSize))
	rh := buzhash64.New()
	rh.Write(make([]byte, 64))
	return &Chunker{
		log2AvgSize: log2AvgSize,
		maxSize:     maxSize,
		onChunk:     onChunk,
		rh:          rh,
	}
}

func (c *Chunker) Write(data []byte) (int, error) {
	var copied int
	for n := range data {
		c.rh.Roll(data[n])
		if atChunkBoundary(c.rh.Sum64(), c.log2AvgSize) || n+c.Buffered() == c.maxSize {
			n2, _ := c.buf.Write(data[copied:n])
			copied += n2
			if err := c.emit(); err != nil {
				return copied, err
			}
		}
	}
	c.buf.Write(data[copied:])
	return len(data), nil
}

func (c *Chunker) emit() error {
	defer func() {
		c.buf.Reset()
		c.rh.Reset()
		c.rh.Write(make([]byte, 64))
	}()
	if c.buf.Len() > 0 {
		return c.onChunk(c.buf.Bytes())
	}
	return nil
}

func (c *Chunker) Buffered() int {
	return c.buf.Len()
}

func (c *Chunker) WriteNoSplit(data []byte) error {
	if len(data) > c.maxSize {
		return errors.Errorf("cannot write data larger than max size")
	}
	// have to split, before
	if c.WouldOverflow(data) {
		if err := c.Flush(); err != nil {
			return err
		}
		return c.WriteNoSplit(data)
	}
	// write then split
	if c.WouldSplit(data) {
		c.buf.Write(data)
		return c.Flush()
	}
	_, err := c.Write(data)
	return err
}

// WouldOverflow returns whether the data would exceed the maximum chunk size, given the buffered data.
func (c *Chunker) WouldOverflow(data []byte) bool {
	return len(data)+c.buf.Len() > c.maxSize
}

// WouldSplit returns whether the data would be split by the chunking algorithm, given the buffered data.
func (c *Chunker) WouldSplit(data []byte) bool {
	rh := buzhash64.New()
	rh.Write(make([]byte, 64))
	for _, b := range c.buf.Bytes() {
		rh.Roll(b)
	}
	for _, b := range data {
		rh.Roll(b)
		if atChunkBoundary(rh.Sum64(), c.log2AvgSize) {
			return true
		}
	}
	return false
}

func (c *Chunker) Flush() error {
	return c.emit()
}

func atChunkBoundary(sum uint64, nbits int) bool {
	return bits.TrailingZeros64(sum) >= nbits
}
