package chunking

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/bits"

	"golang.org/x/crypto/chacha20"
)

const windowSize = 64

type ContentDefined struct {
	minSize, maxSize int
	onChunk          func(data []byte) error
	mask             uint64
	table            [256]uint64

	buf []byte
	end int
	rh  uint64
}

// NewContentDefined creates a new content defined chunker.
// key must not be nil.  Use new([32]byte) to give a key of all zeros.
func NewContentDefined(minSize, avgSize, maxSize int, key *[32]byte, onChunk func(data []byte) error) *ContentDefined {
	if bits.OnesCount(uint(avgSize)) != 1 {
		panic("avgSize must be power of 2")
	}
	if key == nil {
		panic("key must be non-nil")
	}
	if minSize < windowSize {
		panic("minSize must be >= than 64")
	}
	log2AvgSize := bits.TrailingZeros64(uint64(avgSize))

	var nonce [12]byte
	ciph, err := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if err != nil {
		panic(err)
	}
	var table [256]uint64
	for i := 0; i < 256; i++ {
		var keystream [8]byte
		ciph.XORKeyStream(keystream[:], keystream[:])
		table[i] = binary.BigEndian.Uint64(keystream[:])
	}
	c := &ContentDefined{
		minSize: minSize,
		maxSize: maxSize,
		onChunk: onChunk,
		mask:    lowBitsMask(log2AvgSize),
		table:   table,

		buf: make([]byte, maxSize),
	}
	c.Reset()
	return c
}

func (c *ContentDefined) Write(data []byte) (int, error) {
	var total int
	for {
		n, err := c.ingest(data[total:])
		if err != nil {
			return 0, err
		}
		total += n
		if total >= len(data) {
			return total, nil
		}
	}
}

func (c *ContentDefined) ReadFrom(r io.Reader) (int64, error) {
	var total int64
	for {
		n, err := r.Read(c.buf[c.end:])
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		_, err2 := c.Write(c.buf[c.end : c.end+n])
		if err2 != nil {
			return total, err2
		}
		total += int64(n)
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return total, nil
}

func (c *ContentDefined) WriteByte(b byte) error {
	_, err := c.ingest([]byte{b})
	return err
}

func (c *ContentDefined) Buffered() int {
	return c.end
}

func (c *ContentDefined) Reset() {
	c.end = 0
	c.rh = 0
}

func (c *ContentDefined) Flush() error {
	return c.emit()
}

func (c *ContentDefined) MaxSize() int {
	return c.maxSize
}

func (c *ContentDefined) MinSize() int {
	return c.minSize
}

func (c *ContentDefined) MeanSize() int {
	return int(c.mask) + 1
}

// ingest is like Write except returning n < len(data) is acceptable.
func (c *ContentDefined) ingest(data []byte) (int, error) {
	for i, b := range data {
		c.rh = (c.rh << 1) + c.hash(b)
		c.buf[c.end] = b
		c.end++
		if c.atChunkBoundary() {
			return i + 1, c.emit()
		}
	}
	return len(data), nil
}

func (c *ContentDefined) hash(x byte) uint64 {
	return c.table[x]
}

func (c *ContentDefined) emit() error {
	defer c.Reset()
	if c.end > 0 {
		return c.onChunk(c.buf[:c.end])
	}
	return nil
}

func (c *ContentDefined) atChunkBoundary() bool {
	sum := c.rh
	size := c.end
	return size >= c.maxSize || (size >= c.minSize && (sum&c.mask == 0))
}

// lowBitsMask return a uint64 with the low n bits set
func lowBitsMask(n int) uint64 {
	return ^(math.MaxUint64 << n)
}
