package serde

import (
	"encoding/json"
	"encoding/pem"
	"fmt"

	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
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
	case gotfs.Root, *gotfs.Root:
		return "GOTFS ROOT", nil
	case gotvc.Snap, *gotvc.Snap:
		return "GOT SNAPSHOT", nil
	default:
		return "", fmt.Errorf("unknown type %T", x)
	}
}

func marshalPEM(ty string, data []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  ty,
		Bytes: data,
	})
}
