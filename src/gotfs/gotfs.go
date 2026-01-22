package gotfs

import (
	"bytes"
	"fmt"
	"path"
	"strings"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs/gotlob"
	"github.com/gotvc/got/src/gotkv"
)

type (
	Ref    = gotkv.Ref
	Store  = gotkv.Store
	Extent = gotlob.Extent
	Span   = gotkv.Span
)

type Root struct {
	Ref   Ref   `json:"ref"`
	Depth uint8 `json:"depth"`
}

const RootSize = gdat.RefSize + 1

func ParseRoot(data []byte) (*Root, error) {
	var r Root
	if err := r.Unmarshal(data); err != nil {
		return nil, err
	}
	return &r, nil
}

// Marshal appends the root data to out and returns the new slice.
func (r Root) Marshal(out []byte) []byte {
	out = append(out, r.Ref.Marshal()...)
	out = append(out, r.Depth)
	return out
}

// Unmarshal parses the root data from data and returns an error if the data is invalid.
func (r *Root) Unmarshal(data []byte) error {
	if len(data) < RootSize {
		return fmt.Errorf("invalid root length: %d", len(data))
	}
	if err := r.Ref.Unmarshal(data[:gdat.RefSize]); err != nil {
		return err
	}
	r.Depth = data[gdat.RefSize]
	return nil
}

func (r Root) ToGotKV() gotkv.Root {
	return gotkv.Root{
		Ref:   r.Ref,
		First: makeInfoKey(""),
		Depth: r.Depth,
	}
}

func newRoot(x *gotkv.Root) *Root {
	if x == nil {
		return nil
	}
	p, err := parseInfoKey(x.First)
	if err != nil {
		panic(err)
	}
	if p != "" {
		panic(x)
	}
	return &Root{
		Ref:   x.Ref,
		Depth: x.Depth,
	}
}

func (r *Root) toGotKV() *gotkv.Root {
	if r == nil {
		return nil
	}
	r2 := r.ToGotKV()
	return &r2
}

const MaxPathLen = gotkv.MaxKeySize - 2 - 8

// appendPrefix appends the prefix that all information for an object
// will be contained in to out and returns the result.
func appendPrefix(out []byte, p string) []byte {
	p = cleanPath(p)
	out = append(out, Sep)
	out = append(out, []byte(p)...)
	if len(p) != 0 {
		out = append(out, Sep)
	}
	return out
}

// isValidKey returs true if the key is valid.
// This means checking for no NULL bytes until the end.
// And ensuring that the key is at least 9 bytes long.
func isValidKey(k []byte) bool {
	i := bytes.Index(k, []byte{0x00})
	return i > 0 && i == len(k)-9
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

func splitExtentKey(k []byte) (string, uint64, error) {
	if !isExtentKey(k) {
		return "", 0, fmt.Errorf("%q is not an extent key", k)
	}
	prefix, offset, err := gotlob.ParseExtentKey(k)
	if err != nil {
		return "", 0, err
	}
	p := string(prefix[:len(prefix)-1])
	p = cleanPath(p)
	return p, offset, nil
}

func parseExtent(v []byte) (*Extent, error) {
	return gotlob.ParseExtent(v)
}

func makeExtentPrefix(p string) []byte {
	out := appendPrefix(nil, p)
	out = append(out, 0)
	return out
}

func SplitPath(p string) []string {
	p = cleanPath(p)
	return strings.Split(p, "/")
}

func cleanPath(p string) string {
	p = path.Clean(p)
	if p == "." {
		return ""
	}
	return strings.Trim(p, string(Sep))
}

func cleanName(p string) string {
	return strings.Trim(p, string(Sep))
}
