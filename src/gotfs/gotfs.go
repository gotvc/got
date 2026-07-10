package gotfs

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs/internal/gotlob"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotkv/gotkvdelta"
	"github.com/gotvc/got/src/internal/stores"
)

type (
	Ref    = gotkv.Ref
	Extent = gotlob.Extent
)

type Root struct {
	Ref   Ref   `json:"ref"`
	Depth uint8 `json:"depth"`
}

const RootSize = gdat.RefSize + 1

func ParseRoot(data []byte) (Root, error) {
	var r Root
	if err := r.Unmarshal(data); err != nil {
		return Root{}, err
	}
	return r, nil
}

// Marshal appends the root data to out and returns the new slice.
// The format is fixed length.
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
		First: newInfoKey("").Marshal(nil),
		Depth: r.Depth,
	}
}

// Segment returns the root as a single segment.
func (r Root) Segment() Segment {
	return Segment{
		Span:     Span{},
		Contents: r.ToGotKV(),
	}
}

// Equal returns true if a and b contain equivalent data.
func Equal(a, b Root) bool {
	return gdat.Equal(a.Ref, b.Ref) && a.Depth == b.Depth
}

// TODO: remove this method
func newRoot(x gotkv.Root) Root {
	r, err := NewRoot(x)
	if err != nil {
		panic(err)
	}
	return r
}

func NewRoot(x gotkv.Root) (Root, error) {
	if x.Equal(gotkv.Root{}) {
		return Root{}, nil
	}
	var key Key
	if err := unmarshalInfoKey(x.First, &key); err != nil {
		return Root{}, err
	}
	if key.Path() != "" {
		return Root{}, fmt.Errorf("first path must be empty string. HAVE: %q", key.Path())
	}
	return Root{
		Ref:   x.Ref,
		Depth: x.Depth,
	}, nil
}

func (r Root) toGotKV() gotkv.Root {
	if r == (Root{}) {
		return gotkv.Root{}
	}
	return r.ToGotKV()
}

// Span is a range in the filesystem.
// Begin and End are both inclusive.
type Span struct {
	Begin, End Key
}

// TotalSpan returns a Span including all keys
func TotalSpan() Span {
	return Span{}
}

// NewSpan creates a NewSpan from a gotkv.Span by validating the keys.
func NewSpan(x gotkv.Span) (Span, error) {
	beg, err := ParseKey(x.Begin)
	if err != nil {
		return Span{}, err
	}
	if !bytes.HasSuffix(x.End, []byte{0}) {
		return Span{}, fmt.Errorf("gotfs.NewSpan: span end is not suffixed with zero byte")
	}
	end, err := ParseKey(x.Begin)
	if err != nil {
		return Span{}, err
	}
	return Span{Begin: beg, End: end}, nil
}

func ParseSpan(data []byte) (Span, error) {
	if len(data) == 0 {
		return Span{}, nil
	}
	var span gotkv.Span
	if err := span.Unmarshal(data); err != nil {
		return Span{}, err
	}
	var beg, end Key
	if err := beg.Unmarshal(span.Begin); err != nil {
		return Span{}, err
	}
	if err := end.Unmarshal(span.End); err != nil {
		return Span{}, err
	}
	return Span{Begin: beg, End: end}, nil
}

func (sp *Span) AddPrefix(p string) {
	sp.Begin.AddPrefix(p)
	sp.End.AddPrefix(p)
}

// ToSpan returns the gotkv.Span that this Span covers.
func (sp Span) ToSpan() gotkv.Span {
	return gotkv.Span{
		Begin: sp.Begin.Marshal(nil),
		// need to change to exclusive from inclusive End here
		End: gotkv.KeyAfter(sp.End.Marshal(nil)),
	}
}

func (sp Span) Clone() Span {
	return Span{
		Begin: sp.Begin.Clone(),
		End:   sp.End.Clone(),
	}
}

// Segment is a contiguous subset of a GotFS instance.
// It may not be a valid Root.
// The zero valued segment is interpretted as an empty segment.
type Segment struct {
	Span     Span
	Contents gotkv.Root
}

func NewSegment(x gotkvdelta.Segment) (Segment, error) {
	span, err := NewSpan(x.Span)
	if err != nil {
		return Segment{}, err
	}
	return Segment{Span: span, Contents: x.Contents}, nil
}

func (seg Segment) ToSegment() gotkvdelta.Segment {
	return gotkvdelta.Segment{
		Span:     seg.Span.ToSpan(),
		Contents: seg.Contents,
	}
}

func (seg Segment) Marshal(out []byte) []byte {
	seg2 := seg.ToSegment()
	return seg2.Marshal(out)
}

func (seg *Segment) Unmarshal(data []byte) error {
	return nil
}

// Promote promotes a segment to a Root if the segment has the correct first key.
func Promote(ctx context.Context, seg Segment) (Root, error) {
	var key Key
	if err := unmarshalInfoKey(seg.Contents.First, &key); err != nil {
		panic(err)
	}
	if key.Path() != "" {
		return Root{}, fmt.Errorf("segment is not a valid gotfs.Root")
	}
	return Root{
		Ref:   seg.Contents.Ref,
		Depth: seg.Contents.Depth,
	}, nil
}

const MaxPathLen = gotkv.MaxKeySize - 9

func parseExtent(v []byte) (Extent, error) {
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

// RO are read only stores used by gotfs
type RO struct {
	Data     stores.RO
	Metadata stores.RO
}

// WO are read only stores used by gotfs
type WO struct {
	Data     stores.WO
	Metadata stores.WO
}

// RW are read-write stores used by gotfs
type RW struct {
	Data     stores.RW
	Metadata stores.RW
}

func (rw RW) RO() RO {
	return RO{Data: rw.Data, Metadata: rw.Metadata}
}

func (rw RW) WO() WO {
	return WO{Data: rw.Data, Metadata: rw.Metadata}
}
