package gotfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/gotvc/got/src/gotkv"
)

// Key is a key that GotFS stores in GotKV.
// It contains a path and either an Extent end offset or an Info sentinel value.
type Key struct {
	// path contains a null separated path without any leading or trailing separators
	path  []byte
	endAt uint64
}

func newInfoKey(p string) Key {
	p = cleanPath(p)
	pbuf := []byte(p)
	for i := range pbuf {
		if pbuf[i] == Sep {
			pbuf[i] = 0
		}
	}
	return Key{
		path: pbuf,
	}
}

func newExtentKey(p string, endAt uint64) Key {
	k := newInfoKey(p)
	k.endAt = endAt
	return k
}

// IsInfo returns true if the key is for an Info object
func (k *Key) IsInfo() bool {
	return k.endAt == 0
}

// Path returns the path associated with the key.
func (k *Key) Path() string {
	p := string(k.path)
	return strings.ReplaceAll(p, "\x00", string(Sep))
}

// EndAt returns the ending offset for an Extent
func (k *Key) EndAt() uint64 {
	return k.endAt
}

// pathPrefixNoTrail returns the null-separated prefix for a path without the trailing terminator.
func pathPrefixNoTrail(out []byte, p string) []byte {
	k := newInfoKey(p)
	out = k.Prefix(out)
	out = out[:len(out)-1]
	return out
}

// Prefix returns a prefix which all keys for this path, including Infos and Extents will have.
// The prefix will also include any children of the object.
func (k Key) Prefix(out []byte) []byte {
	if len(k.path) > 0 {
		out = append(out, 0)
		out = append(out, k.path...)
	}
	out = append(out, 0)
	return out
}

// ChildrenSpan returns a span that contains all children or the path
// if it was a directory
func (k Key) ChildrenSpan() gotkv.Span {
	beg := k.Marshal(nil)
	return gotkv.Span{
		Begin: beg,
		End:   gotkv.PrefixEnd(k.Prefix(nil)),
	}
}

func (k Key) Marshal(out []byte) []byte {
	out = k.Prefix(out)
	out = binary.BigEndian.AppendUint64(out, k.endAt)
	return out
}

func (k *Key) Unmarshal(data []byte) error {
	if !isValidKey(data) {
		return fmt.Errorf("not a valid key")
	}
	path := data[:len(data)-9]
	if len(path) > 0 && path[0] == 0 {
		path = path[1:]
	}
	k.path = append(k.path[:0], path...)
	k.endAt = binary.BigEndian.Uint64(data[len(data)-8:])
	return nil
}

func unmarshalInfoKey(x []byte, dst *Key) error {
	if !isInfoKey(x) {
		return fmt.Errorf("not a valid metdata key: %q", x)
	}
	return dst.Unmarshal(x)
}

// isValidKey returs true if the key is valid.
// Keys must be >= 9 bytes long
// The 9th byte from the end, must be NULL
// And ensuring that the key is at least 9 bytes long.
func isValidKey(k []byte) bool {
	return len(k) >= 9 && k[len(k)-9] == 0
}

// isInfoKey returns true if k can be interpretted as an info key.
// info keys have 9 NULL bytes at the end.
func isInfoKey(k []byte) bool {
	var infoSuffix = []byte{
		0x00,                                           // null byte
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 8 bytes of zeros
	}
	return bytes.HasSuffix(k, infoSuffix)
}

// isExtentKey returns true if k can be interpretted as an extent key.
// extent keys have the first null byte 9'th from the end and a non-zero 8 byte suffix.
func isExtentKey(k []byte) bool {
	return isValidKey(k) && !isInfoKey(k)
}

func unmarshalExtentKey(k []byte, dst *Key) error {
	if !isExtentKey(k) {
		return fmt.Errorf("%q is not an extent key", k)
	}
	return dst.Unmarshal(k)
}

func checkPath(p string) error {
	if len(p) > MaxPathLen {
		return fmt.Errorf("path too long: %q", p)
	}
	if strings.ContainsAny(p, "\x00") {
		return fmt.Errorf("path cannot contain null")
	}
	return nil
}

func parentPath(x string) string {
	x = cleanPath(x)
	parts := strings.Split(x, string(Sep))
	if len(parts) == 0 {
		panic("no parent of empty path")
	}
	if len(parts) == 1 {
		return ""
	}
	return strings.Join(parts[:len(parts)-1], string(Sep))
}
