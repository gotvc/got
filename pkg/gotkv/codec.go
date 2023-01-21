package gotkv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/gotvc/got/pkg/gotkv/ptree"
)

var _ ptree.Encoder = &Encoder{}

// Encoder is a ptree.Encoder
type Encoder struct {
	prevKey []byte
	count   int
}

func (e *Encoder) WriteEntry(dst []byte, ent Entry) (int, error) {
	prevKey := e.prevKey
	if e.count == 0 {
		prevKey = ent.Key // this will make backspace=0, suffix=0
	}
	if len(dst) < computeEntryLen(prevKey, ent) {
		return 0, ptree.ErrOutOfRoom
	}
	out := appendEntry(dst[:0], prevKey, ent)
	n := len(out)

	e.prevKey = append(e.prevKey[:0], ent.Key...)
	e.count++
	return n, nil
}

func (e *Encoder) EncodedLen(ent Entry) int {
	prevKey := e.prevKey
	if e.count == 0 {
		prevKey = ent.Key // this will make backspace=0, suffix=0
	}
	return computeEntryLen(prevKey, ent)
}

func (e *Encoder) Reset() {
	e.prevKey = e.prevKey[:0]
	e.count = 0
}

// writeLPBytes writes len(x) varint-encoded, followed by x, to buf
func appendLPBytes(out []byte, x []byte) []byte {
	out = binary.AppendUvarint(out, uint64(len(x)))
	return append(out, x...)
}

func appendEntry(out []byte, prevKey []byte, ent Entry) []byte {
	l := computeEntryInnerLen(prevKey, ent)
	out = binary.AppendUvarint(out, uint64(l))
	// key
	cpLen := commonPrefix(prevKey, ent.Key)
	keySuffix := ent.Key[cpLen:]
	backspace := len(prevKey) - cpLen
	out = binary.AppendUvarint(out, uint64(backspace))
	out = appendLPBytes(out, keySuffix)
	// value
	out = appendLPBytes(out, ent.Value)

	return out
}

// computeEntryLen returns the number of bytes appended by appendEntry
func computeEntryLen(prevKey []byte, ent Entry) int {
	l := computeEntryInnerLen(prevKey, ent)
	return l + int(varintLen(uint64(l)))
}

// computeEntryInnerLen return the number of bytes that the entry returns, not including it's length prefix.
func computeEntryInnerLen(prevKey []byte, ent Entry) (total int) {
	cpLen := commonPrefix(prevKey, ent.Key)
	keySuffix := ent.Key[cpLen:]
	backspace := uint32(len(prevKey) - cpLen)

	// key backspace
	total += varintLen(uint64(backspace))
	// key suffix
	total += varintLen(uint64(len(keySuffix)))
	total += len(keySuffix)
	// value
	total += varintLen(uint64(len(ent.Value)))
	total += len(ent.Value)
	return total
}

func varintLen(x uint64) int {
	buf := [binary.MaxVarintLen64]byte{}
	return binary.PutUvarint(buf[:], x)
}

type Decoder struct {
	prevKey []byte
	count   int
}

func (d *Decoder) ReadEntry(src []byte, ent *Entry) (int, error) {
	n, err := readEntry(ent, src, d.prevKey)
	if err != nil {
		return 0, err
	}
	d.prevKey = append(d.prevKey[:0], ent.Key...)
	d.count++
	return n, nil
}

func (d *Decoder) Reset(parentKey []byte) {
	d.prevKey = append(d.prevKey[:0], parentKey...)
	d.count = 0
}

// readEntry reads an entry into ent
func readEntry(ent *Entry, src []byte, prevKey []byte) (nRead int, _ error) {
	innerLen, n := binary.Uvarint(src)
	if err := checkVarint(n); err != nil {
		return 0, err
	}
	maxSize := uint64(len(src) - n)
	if innerLen > maxSize {
		return 0, fmt.Errorf("entry exceeds max size: %d > %d", innerLen, maxSize)
	}
	entryLen := int(innerLen) + n
	nRead += n
	// key
	n, err := readKey(ent, src[nRead:], prevKey)
	if err != nil {
		return entryLen, err
	}
	nRead += n
	// value
	ent.Value = ent.Value[:0]
	ent.Value, n, err = readLPBytes(ent.Value, src[nRead:], math.MaxUint32)
	if err != nil {
		return entryLen, err
	}
	nRead += n
	// check we read the right amount
	if nRead != entryLen {
		return entryLen, fmt.Errorf("invalid entry, %d leftover bytes", entryLen-n)
	}
	return nRead, nil
}

// readKey reads 1 varint for the backspace, and 1 length-prefixed bytes
// for the key suffix
func readKey(ent *Entry, src []byte, prevKey []byte) (nRead int, err error) {
	keyBackspace, n := binary.Uvarint(src)
	if err := checkVarint(n); err != nil {
		return 0, err
	}
	if int(keyBackspace) > len(prevKey) {
		return 0, fmt.Errorf("backspace is > len(prevKey): prevKey=%q bs=%d", prevKey, keyBackspace)
	}
	nRead += n

	end := len(prevKey) - int(keyBackspace)
	ent.Key = ent.Key[:0]
	ent.Key = append(ent.Key, prevKey[:end]...)

	ent.Key, n, err = readLPBytes(ent.Key, src[nRead:], MaxKeySize)
	if err != nil {
		return 0, err
	}
	nRead += n
	return nRead, nil
}

func checkVarint(n int) error {
	if n == 0 {
		return errors.New("buffer too small")
	}
	if n < 0 {
		return errors.New("varint too big")
	}
	return nil
}

// readLPBytes reads a varint from int, and then appends that many bytes from in to out,
// excluding the bytes read for the varint
// it returns the new slice, or an error.
func readLPBytes(out []byte, in []byte, maxLen int) ([]byte, int, error) {
	l, n := binary.Uvarint(in)
	if err := checkVarint(n); err != nil {
		return nil, 0, err
	}
	maxLen = min(maxLen, len(in)-n)
	if l > uint64(maxLen) {
		return nil, 0, fmt.Errorf("lp bytestring exceeds max size %d > %d", l, maxLen)
	}
	out = append(out, in[n:n+int(l)]...)
	return out, int(l) + n, nil
}

func commonPrefix(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

func min(a, b int) int {
	if b < a {
		return b
	}
	return a
}
