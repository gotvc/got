package gotfs

import "encoding/json"

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
