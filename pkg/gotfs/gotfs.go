package gotfs

import (
	"bytes"
	"fmt"
	"path"
	"strings"

	"github.com/gotvc/got/pkg/gotfs/gotlob"
	"github.com/gotvc/got/pkg/gotkv"
)

type (
	Ref    = gotkv.Ref
	Store  = gotkv.Store
	Root   = gotkv.Root
	Extent = gotlob.Extent
	Span   = gotkv.Span
)

const MaxPathLen = gotkv.MaxKeySize - 2 - 8

// Segment is a span of a GotFS instance.
type Segment struct {
	Span gotkv.Span
	Root Root
}

func (s Segment) String() string {
	return fmt.Sprintf("{ %v : %v}", s.Span, s.Root.Ref.CID)
}

// isInfoKey returns true if k can be interpretted as an info key.
// info keys have no null bytes in the key
func isInfoKey(k []byte) bool {
	return !bytes.Contains(k, []byte{0x00})
}

// isExtentKey returns true if k can be interpretted as an extent key.
// extent keys have the first null byte 9'th from the end.
func isExtentKey(k []byte) bool {
	i := bytes.Index(k, []byte{0x00})
	return i > 0 && i == len(k)-9
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
