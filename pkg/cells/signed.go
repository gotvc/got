package cells

import (
	"context"
	"encoding/binary"

	"github.com/brendoncarroll/go-p2p"
	"github.com/pkg/errors"
)

type Signed struct {
	inner      Cell
	purpose    string
	publicKey  p2p.PublicKey
	privateKey p2p.PrivateKey
}

func NewSigned(inner Cell, purpose string, publicKey p2p.PublicKey, privateKey p2p.PrivateKey) Cell {
	if publicKey == nil {
		panic("must specify public key")
	}
	return &Signed{
		inner:     inner,
		publicKey: publicKey,
	}
}

func (s *Signed) CAS(ctx context.Context, prev, next []byte) (bool, []byte, error) {
	if s.privateKey == nil {
		return false, nil, errors.Errorf("cannot write to signing cell without key")
	}
	panic("not implemented")
}

func (s *Signed) Get(ctx context.Context) ([]byte, error) {
	data, err := s.inner.Get(ctx)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, errors.Errorf("data too short")
	}
	payloadLength := int(binary.BigEndian.Uint32(data[:4]))
	if payloadLength+4 > len(data) {
		return nil, errors.Errorf("incorrect payload length")
	}
	content := data[4 : 4+payloadLength]
	sig := data[4+payloadLength:]
	if err := p2p.Verify(s.publicKey, s.purpose, content, sig); err != nil {
		return nil, err
	}
	return content, nil
}
