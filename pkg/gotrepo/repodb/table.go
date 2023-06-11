package repodb

import (
	"encoding/binary"
)

type TableID uint32

func (tid TableID) AppendTo(x []byte) []byte {
	return binary.BigEndian.AppendUint32(x, uint32(tid))
}

func (tid TableID) Bytes() []byte {
	return tid.AppendTo(nil)
}

func (tid TableID) ByteArray() (ret [4]byte) {
	binary.BigEndian.PutUint32(ret[:], uint32(tid))
	return ret
}

func IDFromString(x string) TableID {
	if len(x) > 4 {
		panic(x)
	}
	var buf [4]byte
	copy(buf[:], x)
	return TableID(binary.BigEndian.Uint32(buf[:]))
}

func Key(out []byte, tid TableID, key []byte) []byte {
	out = tid.AppendTo(out)
	out = append(out, key...)
	return out
}
