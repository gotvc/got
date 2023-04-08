package gotkv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/gotvc/got/pkg/gotkv/ptree"
	"github.com/gotvc/got/pkg/maybe"
)

type Index = ptree.Index[kvstreams.Entry, gdat.Ref]

var _ ptree.Encoder[kvstreams.Entry] = &Encoder{}

// Encoder is a ptree.Encoder
type Encoder struct {
	prevKey []byte
	count   int
}

func (e *Encoder) Write(dst []byte, ent Entry) (int, error) {
	prevKey := e.prevKey
	if e.count == 0 {
		prevKey = ent.Key // this will make backspace=0, suffix=""
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
		prevKey = ent.Key // this will make backspace=0, suffix=""
	}
	return computeEntryLen(prevKey, ent)
}

func (e *Encoder) Reset() {
	e.prevKey = e.prevKey[:0]
	e.count = 0
}

var _ ptree.Encoder[ptree.Index[Entry, Ref]]

// IndexEncoder encodes indexes
type IndexEncoder struct {
	Encoder
}

func (e *IndexEncoder) Write(dst []byte, x ptree.Index[Entry, Ref]) (int, error) {
	return e.Encoder.Write(dst, Entry{
		Key:   x.First.X.Key,
		Value: gdat.AppendRef(nil, x.Ref),
	})
}

func (e *IndexEncoder) EncodedLen(x Index) int {
	return e.Encoder.EncodedLen(Entry{
		Key:   x.First.X.Key,
		Value: gdat.AppendRef(nil, x.Ref),
	})
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

var _ ptree.Decoder[Entry, Ref] = &Decoder{}

type Decoder struct {
	prevKey []byte
}

func newDecoder() ptree.Decoder[Entry, Ref] {
	return &Decoder{}
}

func (d *Decoder) Read(src []byte, ent *Entry) (int, error) {
	n, err := readEntry(ent, src, d.prevKey)
	if err != nil {
		return 0, err
	}
	d.prevKey = append(d.prevKey[:0], ent.Key...)
	return n, nil
}

func (d *Decoder) Peek(src []byte, ent *Entry) error {
	_, err := readEntry(ent, src, d.prevKey)
	return err
}

func (d *Decoder) Reset(parent Index) {
	if parent.First.Ok {
		d.prevKey = append(d.prevKey[:0], parent.First.X.Key...)
	} else {
		d.prevKey = d.prevKey[:0]
	}
}

var _ ptree.Decoder[Index, Ref] = &IndexDecoder{}

type IndexDecoder struct {
	parent  Index
	prevKey []byte
}

func newIndexDecoder() ptree.Decoder[Index, Ref] {
	return &IndexDecoder{}
}

func (d *IndexDecoder) Read(src []byte, dst *Index) (int, error) {
	n, err := d.readIndex(src, dst)
	if err != nil {
		return 0, err
	}
	d.setPrevKey(dst.First.X.Key)
	return n, nil
}

func (d *IndexDecoder) Peek(src []byte, dst *Index) error {
	_, err := d.readIndex(src, dst)
	return err
}

func (d *IndexDecoder) Reset(parent ptree.Index[Index, Ref]) {
	d.parent = ptree.FlattenIndex(parent)
	d.setPrevKey(d.parent.First.X.Key)
}

func (d *IndexDecoder) readIndex(src []byte, dst *Index) (int, error) {
	var ent1, ent2 Entry
	n1, n2, err := read2Entries(src, d.prevKey, &ent1, &ent2)
	if err != nil {
		return 0, err
	}
	dst.First = maybe.Just(Entry{Key: ent1.Key})
	ref, err := gdat.ParseRef(ent1.Value)
	if err != nil {
		return 0, err
	}
	dst.Ref = ref
	dst.Last = maybe.Nothing[Entry]()
	if n2 > 0 {
		//dst.Last = maybe.Just(Entry{Key: ent2.Key})
		// TODO: we incorrectly assume that nodes not at the right edge of the tree are always natural
		// They may not be in cases of an entry exceeding the maximum size
		dst.IsNatural = false
	} else {
		//dst.Last = d.parent.Last.Clone(copyEntry)
		dst.IsNatural = false
	}
	return n1, nil
}

func (d *IndexDecoder) setPrevKey(x []byte) {
	d.prevKey = append(d.prevKey[:0], x...)
}

func read2Entries(src []byte, prevKey []byte, ent1, ent2 *Entry) (int, int, error) {
	n1, err := readEntry(ent1, src, prevKey)
	if err != nil {
		return 0, 0, err
	}
	if len(src[n1:]) == 0 {
		return n1, 0, nil
	}
	n2, err := readEntry(ent2, src[n1:], ent1.Key)
	if err != nil {
		return n1, 0, err
	}
	return n1, n2, nil
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
		return errors.New("reading varint: buffer too small")
	}
	if n < 0 {
		return errors.New("reading varint: varint too big")
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
