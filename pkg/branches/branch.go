package branches

import (
	"time"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cells"
)

// Volume is a Cell and a set of stores
type Volume struct {
	cells.Cell
	VCStore, FSStore, RawStore cadata.Store
}

type Annotations = map[string]string

// Branch is a Volume plus additional metadata
type Branch struct {
	Volume      Volume
	Annotations Annotations
	CreatedAt   time.Time
}
