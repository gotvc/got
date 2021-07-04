package got

import (
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/got/pkg/branches"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/brendoncarroll/got/pkg/gotvc"
)

type (
	FS = fs.FS

	Cell   = cells.Cell
	Realm  = branches.Realm
	Volume = branches.Volume
	Branch = branches.Branch
	Store  = cadata.Store

	Ref  = gotkv.Ref
	Root = gotfs.Root

	Commit = gotvc.Snapshot
)

func MarshalPEM(x interface{}) ([]byte, error) {
	ty, err := getPEMType(x)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(x)
	if err != nil {
		return nil, err
	}
	return marshalPEM(ty, data), nil
}

func getPEMType(x interface{}) (string, error) {
	switch x := x.(type) {
	case Root, *Root:
		return "GOTFS ROOT", nil
	case Commit, *Commit:
		return "GOT COMMIT", nil
	default:
		return "", errors.Errorf("unknown type %T", x)
	}
}
