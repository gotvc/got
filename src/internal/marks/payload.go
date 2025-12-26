package marks

import (
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/sbe"
)

type Snap = gotvc.Snapshot[Payload]

// Payload is the thing being snapshotted.
type Payload struct {
	Root gotfs.Root
	Aux  []byte
}

func ParsePayload(data []byte) (Payload, error) {
	var ret Payload
	if err := ret.Unmarshal(data); err != nil {
		return ret, err
	}
	return ret, nil
}

func (p Payload) Marshal(out []byte) []byte {
	out = p.Root.Marshal(out)
	out = sbe.AppendLP(out, p.Aux)
	return out
}

func (p *Payload) Unmarshal(data []byte) error {
	rootData, data, err := sbe.ReadN(data, gotfs.RootSize)
	if err != nil {
		return err
	}
	root, err := gotfs.ParseRoot(rootData)
	if err != nil {
		return err
	}
	p.Root = *root
	auxData, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	p.Aux = auxData
	return nil
}
