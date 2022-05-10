package gotobj

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/brendoncarroll/go-state"
	"github.com/gotvc/got/pkg/gotkv"
)

type (
	Ref   = gotkv.Ref
	Store = gotkv.Store
	Root  = gotkv.Root
	Span  = state.ByteSpan
)

type Segment struct {
	Root Root
	Span Span
}

func (s Segment) String() string {
	return fmt.Sprintf("{ %v : %v}", s.Span, s.Root.Ref.CID)
}

func PutInline(b *Builder, key []byte, data []byte) error {
	if err := b.Begin(key, true); err != nil {
		return err
	}
	_, err := b.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func parseKey(x []byte) ([]byte, uint64, error) {
	if len(x) < 8 {
		return nil, 0, errors.New("key is too short")
	}
	l := len(x)
	return x[:l-8], binary.BigEndian.Uint64(x[l-8:]), nil
}

func makeKey(k []byte, o uint64) []byte {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], o)
	return append(k, buf[:]...)
}

type value struct {
	Literal []byte
	Extent  *Extent
}

func parseValue(x []byte) (*value, error) {
	return &value{}, nil
}
