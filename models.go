package got

import (
	"encoding/json"

	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/cells"
	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/brendoncarroll/got/pkg/gotkv"
	"github.com/brendoncarroll/got/pkg/realms"
	"github.com/pkg/errors"
)

type FS = fs.FS

type Cell = cells.Cell

type Env = realms.Env

type Realm = realms.Realm

type Store = cadata.Store

type Ref = gotkv.Ref

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
	case Ref, *Ref:
		return "GOT REF", nil
	case Commit, *Commit:
		return "GOT COMMIT", nil
	default:
		return "", errors.Errorf("unknown type %T", x)
	}
}
