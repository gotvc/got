package gotfs

import (
	"encoding/binary"
	"encoding/json"

	"github.com/pkg/errors"
)

type Part struct {
	Ref    Ref    `json:"ref"`
	Offset uint32 `json:"offset,omitempty"`
	Length uint32 `json:"length,omitempty"`
}

func (p *Part) marshal() []byte {
	data, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return data
}

func parsePart(data []byte) (*Part, error) {
	var p Part
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func splitPartKey(k []byte) (p string, offset uint64, err error) {
	if len(k) < 9 {
		return "", 0, errors.Errorf("key too short")
	}
	if k[len(k)-9] != 0x00 {
		return "", 0, errors.Errorf("not part key, no NULL")
	}
	p = string(k[:len(k)-9])
	offset = binary.BigEndian.Uint64(k[len(k)-8:])
	return p, offset, nil
}

func makePartKey(p string, offset uint64) []byte {
	x := []byte(p)
	x = append(x, 0x00)
	x = appendUint64(x, offset)
	return x
}
