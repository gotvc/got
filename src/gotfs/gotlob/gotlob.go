package gotlob

import (
	"fmt"

	"github.com/gotvc/got/src/gotkv"
)

type (
	Ref   = gotkv.Ref
	Store = gotkv.Store
	Root  = gotkv.Root
	Span  = gotkv.Span
)

type Segment struct {
	Root Root
	Span Span
}

func (s Segment) String() string {
	return fmt.Sprintf("{ %v : %v}", s.Span, s.Root.Ref.CID)
}
