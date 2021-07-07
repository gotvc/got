package got

import (
	"encoding/json"
	"encoding/pem"

	"github.com/brendoncarroll/got/pkg/gdat"
	"github.com/brendoncarroll/got/pkg/gotfs"
	"github.com/brendoncarroll/got/pkg/gotrepo"
	"github.com/pkg/errors"
)

type (
	Repo     = gotrepo.Repo
	Root     = gotfs.Root
	Ref      = gdat.Ref
	SnapInfo = gotrepo.SnapInfo
	Snap     = gotrepo.Snap
)

func InitRepo(p string) error {
	return gotrepo.Init(p)
}

func OpenRepo(p string) (*Repo, error) {
	return gotrepo.Open(p)
}

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
	case Snap, *Snap:
		return "GOT SNAPSHOT", nil
	default:
		return "", errors.Errorf("unknown type %T", x)
	}
}

func marshalPEM(ty string, data []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  ty,
		Bytes: data,
	})
}
