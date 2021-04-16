package got

import (
	"encoding/json"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/brendoncarroll/got/pkg/gotvc"
	"github.com/brendoncarroll/got/pkg/volumes"
	"github.com/pkg/errors"
)

type (
	FS = fs.FS

	Cell   = cells.Cell
	Volume = volumes.Volume
	Realm  = volumes.Realm
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
