// package sbe implements simple binary encoding formats for serializing and deserializing data.
package sbe

import (
	"encoding/binary"
	"fmt"
)

func AppendUint64(out []byte, x uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], x)
	return append(out, buf[:]...)
}

func ReadUint64(data []byte) (uint64, []byte, error) {
	if len(data) < 8 {
		return 0, nil, fmt.Errorf("too short to contain uint64")
	}
	return binary.LittleEndian.Uint64(data[:8]), data[8:], nil
}

func AppendUint32(out []byte, x uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], x)
	return append(out, buf[:]...)
}

func AppendUint16(out []byte, x uint16) []byte {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], x)
	return append(out, buf[:]...)
}

func ReadUint16(data []byte) (uint16, []byte, error) {
	if len(data) < 2 {
		return 0, nil, fmt.Errorf("too short to contain uint16")
	}
	return binary.LittleEndian.Uint16(data[:2]), data[2:], nil
}

// AppendLP appends a length-prefixed byte slice to out.
// the length is encoded as a varint
func AppendLP(out []byte, x []byte) []byte {
	out = binary.AppendUvarint(out, uint64(len(x)))
	return append(out, x...)
}

// ReadLP reads a length-prefixed byte slice from data.
// ReadLP reads the format output by AppendLP.
func ReadLP(x []byte) (ret []byte, rest []byte, _ error) {
	l, n := binary.Uvarint(x)
	if n <= 0 {
		return nil, nil, fmt.Errorf("too short to contain lp")
	}
	return x[n : n+int(l)], x[n+int(l):], nil
}

// AppendLP16 appends a length-prefixed byte slice to out.
// the length is encoded as a 16-bit little-endian integer.
func AppendLP16(out []byte, x []byte) []byte {
	out = AppendUint16(out, uint16(len(x)))
	return append(out, x...)
}

// ReadLP16 reads a length-prefixed byte slice from data.
// ReadLP16 reads the format output by AppendLP16.
func ReadLP16(x []byte) (ret []byte, rest []byte, _ error) {
	n, rest, err := ReadUint16(x)
	if err != nil {
		return nil, nil, err
	}
	if len(rest) < int(n) {
		return nil, nil, fmt.Errorf("too short to contain lp16")
	}
	return rest[:n], rest[n:], nil
}

// ReadN reads n bytes from data.
func ReadN(data []byte, n int) ([]byte, []byte, error) {
	if len(data) < n {
		return nil, nil, fmt.Errorf("cannot read %d bytes from %d bytes", n, len(data))
	}
	return data[:n], data[n:], nil
}
