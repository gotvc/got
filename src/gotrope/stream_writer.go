package gotrope

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dchest/siphash"
)

type IndexCallback[Ref any] func(context.Context, Index[Ref]) error

type StreamWriter[Ref any] struct {
	s        WriteStorage[Ref]
	meanSize int
	maxSize  int
	cb       IndexCallback[Ref]
	seed     *[16]byte

	weight Weight
	buf    []byte
}

func NewStreamWriter[Ref any](s WriteStorage[Ref], meanSize, maxSize int, seed *[16]byte, cb IndexCallback[Ref]) *StreamWriter[Ref] {
	if meanSize > maxSize {
		panic(fmt.Sprintf("%d > %d", meanSize, maxSize))
	}
	if s.MaxSize() < maxSize {
		maxSize = s.MaxSize()
	}
	if seed == nil {
		seed = new([16]byte)
	}
	return &StreamWriter[Ref]{
		s:        s,
		meanSize: meanSize,
		maxSize:  maxSize,
		cb:       cb,
		seed:     seed,

		buf: make([]byte, 0, maxSize),
	}
}

func (sw *StreamWriter[Ref]) Append(ctx context.Context, se StreamEntry) error {
	l := entryEncodedLen(se.Weight, se.Value)
	if l > sw.maxSize {
		return fmt.Errorf("data exceeds max node size. %d > %d", l, sw.maxSize)
	}
	if len(sw.buf)+l > sw.maxSize {
		if err := sw.Flush(ctx); err != nil {
			return err
		}
	}
	sw.buf = appendEntry(sw.buf, se)
	entryData := sw.buf[len(sw.buf)-l:]
	sw.weight.Add(sw.weight, se.Weight)
	if sw.isSplitPoint(entryData) {
		return sw.Flush(ctx)
	}
	return nil
}

func (sw *StreamWriter[Ref]) Flush(ctx context.Context) error {
	ref, err := sw.s.Post(ctx, sw.buf)
	if err != nil {
		return err
	}
	defer func() {
		sw.buf = sw.buf[:0]
		sw.weight = sw.weight[:0]
	}()
	return sw.cb(ctx, Index[Ref]{
		Ref:    ref,
		Weight: sw.weight,
	})
}

func (sw *StreamWriter[Ref]) Buffered() int {
	return len(sw.buf)
}

func (sw *StreamWriter[Ref]) isSplitPoint(entryData []byte) bool {
	r := hash64(sw.seed, entryData)
	prob := math.MaxUint64 / uint64(sw.meanSize) * uint64(len(entryData))
	return r < prob
}

// appendEntry appends an entry to out
// varint | 1 byte indent | variable length data |
func appendEntry(out []byte, se StreamEntry) []byte {
	out = appendVarint(out,
		uint64(weightEncodedLen(se.Weight))+
			uint64(lpEncodedLen(len(se.Value))),
	)
	out = appendWeight(out, se.Weight)
	out = appendLP(out, se.Value)
	return out
}

// entryEncodedLen is the number of bytes appendEntry will append.
func entryEncodedLen(w Weight, data []byte) int {
	return lpEncodedLen(weightEncodedLen(w) + lpEncodedLen(len(data)))
}

func weightEncodedLen(w Weight) (ret int) {
	// TODO: compress leading 0s
	var total int
	for i := range w {
		total += varintLen(w[i])
	}
	return lpEncodedLen(total)
}

func appendWeight(out []byte, w Weight) []byte {
	var total int
	for i := range w {
		total += varintLen(w[i])
	}

	out = appendVarint(out, uint64(total))
	for i := range w {
		out = appendVarint(out, w[i])
	}
	return out
}

// appendLP appends a length prefixed x to out and returns the result
func appendLP(out []byte, x []byte) []byte {
	out = appendVarint(out, uint64(len(x)))
	out = append(out, x...)
	return out
}

// lpEncodedLen is the total length of a length-prefixed string of length dataLen
func lpEncodedLen(dataLen int) int {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(dataLen))
	return n + dataLen
}

// appendVarint appends x varint-encoded to out and returns the result.
func appendVarint(out []byte, x uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], x)
	return append(out, buf[:n]...)
}

// varintLen returns the number of bytes it would take to encode x as a varint
func varintLen(x uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], x)
}

func hash64(key *[16]byte, data []byte) uint64 {
	en := binary.LittleEndian
	k0 := en.Uint64(key[:8])
	k1 := en.Uint64(key[8:])
	return siphash.Hash(k0, k1, data)
}
