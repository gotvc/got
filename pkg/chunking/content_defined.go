package chunking

import (
	"bytes"
	"math/bits"

	"github.com/chmduquesne/rollinghash/rabinkarp64"
	"golang.org/x/crypto/blake2b"
)

const windowSize = 64

type rollingHash = rabinkarp64.RabinKarp64

type ContentDefined struct {
	avgBits          int
	minSize, maxSize int
	onChunk          func(data []byte) error
	rh               *rollingHash
	buf              bytes.Buffer
}

func NewContentDefined(minSize, avgSize, maxSize int, pol rabinkarp64.Pol, onChunk func(data []byte) error) *ContentDefined {
	if bits.OnesCount(uint(avgSize)) != 1 {
		panic("avgSize must be power of 2")
	}
	if pol == 0 {
		panic("pol must be non-zero")
	}
	if minSize < windowSize {
		panic("minSize must be larger than 64")
	}
	log2AvgSize := bits.TrailingZeros64(uint64(avgSize))
	rh := rabinkarp64.NewFromPol(pol)
	c := &ContentDefined{
		avgBits: log2AvgSize,
		minSize: minSize,
		maxSize: maxSize,
		onChunk: onChunk,
		rh:      rh,
	}
	c.Reset()
	return c
}

func (c *ContentDefined) Write(data []byte) (int, error) {
	for i := range data {
		if err := c.WriteByte(data[i]); err != nil {
			return i, err
		}
		if c.atChunkBoundary(c.rh, &c.buf) {
			if err := c.emit(); err != nil {
				return i, err
			}
		}
	}
	return len(data), nil
}

func (c *ContentDefined) WriteByte(b byte) error {
	roll(c.rh, &c.buf, b)
	return nil
}

func (c *ContentDefined) atChunkBoundary(rh *rollingHash, buf *bytes.Buffer) bool {
	return buf.Len() >= c.minSize && (atChunkBoundary(rh.Sum64(), c.avgBits) || buf.Len() >= c.maxSize)
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
}

func (c *ContentDefined) Flush() error {
	return c.emit()
}

func roll(rh *rollingHash, buf *bytes.Buffer, b byte) {
	if buf.Len() < windowSize {
		rh.Write([]byte{b})
	} else {
		rh.Roll(b)
	}
	buf.WriteByte(b)
}

func atChunkBoundary(sum uint64, nbits int) bool {
	return bits.TrailingZeros64(sum) >= nbits
}

func DerivePolynomial(seed []byte) rabinkarp64.Pol {
	const purpose = "rabinkarp64"
	xof, err := blake2b.NewXOF(blake2b.OutputLengthUnknown, seed)
	if err != nil {
		panic(err)
	}
	xof.Write([]byte(purpose))
	poly, err := rabinkarp64.DerivePolynomial(xof)
	if err != nil {
		panic(err)
	}
	return poly
}
