package branches

import (
	"regexp"
	"time"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cells"
	"github.com/pkg/errors"
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

var nameRegExp = regexp.MustCompile(`^[\w- =.]+$`)

func IsValidName(name string) bool {
	return nameRegExp.MatchString(name)
}

func CheckName(name string) error {
	if IsValidName(name) {
		return nil
	}
	return errors.Errorf("%q is not a valid branch name", name)
}
