package gotlob

import (
	"fmt"

	"github.com/gotvc/got/pkg/gotkv"
)

type (
	Ref   = gotkv.Ref
	Store = gotkv.Store
	Root  = gotkv.Root
	Span  = gotkv.Span
)

const (
	DefaultMaxBlobSize = 1 << 21

	DefaultMinBlobSizeData     = 1 << 12
	DefaultAverageBlobSizeData = 1 << 20

	DefaultAverageBlobSizeKV = 1 << 13
)

type Segment struct {
	Root Root
	Span Span
}

func (s Segment) String() string {
	return fmt.Sprintf("{ %v : %v}", s.Span, s.Root.Ref.CID)
}
