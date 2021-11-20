package tai64

import (
	"encoding/binary"
	"time"

	"github.com/vektra/tai64n"
)

// TAI64 is a TAI64 timestamp
type TAI64 uint64

func Now() TAI64 {
	return TAI64FromGoTime(time.Now())
}

func TAI64FromGoTime(x time.Time) TAI64 {
	return TAI64(tai64n.FromTime(x).Seconds)
}

func (t TAI64) GoTime() time.Time {
	t2 := tai64n.TAI64N{Seconds: uint64(t)}
	return t2.Time()
}

func (t TAI64) String() string {
	return t.GoTime().Local().String()
}

// ExternalFormat returns the "External TAI64 format"
// as defined by https://cr.yp.to/libtai/tai64.html
func (t TAI64) ExternalFormat() (ret [8]byte) {
	binary.BigEndian.PutUint64(ret[:], uint64(t))
	return ret
}

type TAI64N struct {
	TAI64
	Nanoseconds uint32
}

type TAI64NA struct {
	TAI64N
	Attoseconds uint32
}
