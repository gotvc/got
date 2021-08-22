package chunking

import (
	"bytes"
	"math/bits"

	"github.com/chmduquesne/rollinghash/buzhash64"
	"github.com/pkg/errors"
)

type ContentDefined struct {
	log2AvgSize, maxSize int
	onChunk              func(data []byte) error
	hashes               *[256]uint64
	rh                   *buzhash64.Buzhash64
	buf                  bytes.Buffer
}

func NewContentDefined(avgSize, maxSize int, hashes *[256]uint64, onChunk func(data []byte) error) *ContentDefined {
	if bits.OnesCount(uint(avgSize)) != 1 {
		panic("avgSize must be power of 2")
	}
	if hashes == nil {
		hs := buzhash64.GenerateHashes(1)
		hashes = &hs
	}
	log2AvgSize := bits.TrailingZeros64(uint64(avgSize))
	rh := buzhash64.NewFromUint64Array(*hashes)
	rh.Write(make([]byte, 64))
	return &ContentDefined{
		log2AvgSize: log2AvgSize,
		maxSize:     maxSize,
		onChunk:     onChunk,
		rh:          rh,
		hashes:      hashes,
	}
}

func (c *ContentDefined) Write(data []byte) (int, error) {
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

func (c *ContentDefined) emit() error {
	defer func() {
		c.Reset()

	}()
	if c.buf.Len() > 0 {
		return c.onChunk(c.buf.Bytes())
	}
	return nil
}

func (c *ContentDefined) Buffered() int {
	return c.buf.Len()
}

func (c *ContentDefined) Reset() {
	c.buf.Reset()
	c.rh.Reset()
	c.rh.Write(make([]byte, 64))
}

func (c *ContentDefined) WriteNoSplit(data []byte) error {
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
func (c *ContentDefined) WouldOverflow(data []byte) bool {
	return len(data)+c.buf.Len() > c.maxSize
}

// WouldSplit returns whether the data would be split by the chunking algorithm, given the buffered data.
func (c *ContentDefined) WouldSplit(data []byte) bool {
	rh := buzhash64.NewFromUint64Array(*c.hashes)
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

func (c *ContentDefined) Flush() error {
	return c.emit()
}

func atChunkBoundary(sum uint64, nbits int) bool {
	return bits.TrailingZeros64(sum) >= nbits
}
