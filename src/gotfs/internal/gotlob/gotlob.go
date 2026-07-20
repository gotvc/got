package gotlob

import (
<<<<<<< HEAD
	"fmt"

	"github.com/gotvc/got/src/gotkv"
=======
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotkv/gotkvdelta"
>>>>>>> 34d8002 (gotfs+gotkv: add delta packages)
)

type (
	Ref   = gotkv.Ref
	Store = gotkv.Store
	Root  = gotkv.Root
	Span  = gotkv.Span
)

<<<<<<< HEAD
type Segment struct {
	Root Root
	Span Span
}

func (s Segment) String() string {
	return fmt.Sprintf("{ %v : %v}", s.Span, s.Root.Ref.CID)
}
=======
type Segment = gotkvdelta.Segment
>>>>>>> 34d8002 (gotfs+gotkv: add delta packages)
