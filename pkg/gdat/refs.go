package gdat

import (
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/base64"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/metrics"
	"github.com/pkg/errors"
)

const (
	MaxRefBinaryLen = cadata.IDSize + DEKSize
	Base64Alphabet  = cadata.Base64Alphabet
)

var codec = base64.NewEncoding(Base64Alphabet).WithPadding(base64.NoPadding)

type Ref struct {
	CID cadata.ID
	DEK DEK
}

func (r Ref) MarshalText() ([]byte, error) {
	buf := [128]byte{}
	var n int
	codec.Encode(buf[:], r.CID[:])
	n += codec.EncodedLen(len(r.CID[:]))
	buf[n] = '#'
	n++
	codec.Encode(buf[n:], r.DEK[:])
	n += codec.EncodedLen(len(r.DEK[:]))
	return buf[:n], nil
}

func (r *Ref) UnmarshalText(data []byte) error {
	parts := bytes.SplitN(data, []byte("#"), 2)
	if len(parts) != 2 {
		return errors.Errorf("invalid ref")
	}
	cid, dek := parts[0], parts[1]
	if n, err := codec.Decode(r.CID[:], cid); err != nil {
		return err
	} else if n != len(r.CID) {
		return errors.Errorf("wrong length for CID")
	}
	if n, err := codec.Decode(r.DEK[:], dek); err != nil {
		return err
	} else if n != len(r.DEK) {
		return errors.Errorf("wrong length for DEK")
	}
	return nil
}

func (r Ref) MarshalBinary() ([]byte, error) {
	buf := make([]byte, len(r.CID)+len(r.DEK))
	n := copy(buf, r.CID[:])
	copy(buf[n:], r.DEK[:])
	return buf, nil
}

func (ref *Ref) UnmarshalBinary(x []byte) error {
	if len(x) != len(ref.CID)+len(ref.DEK) {
		return errors.Errorf("wrong length for ref %d %q", len(x), x)
	}
	copy(ref.CID[:], x[:len(ref.CID)])
	copy(ref.DEK[:], x[len(ref.CID):])
	return nil
}

func (r Ref) String() string {
	data, _ := r.MarshalText()
	return string(data)
}

func (r Ref) IsZero() bool {
	for i := range r.CID {
		if r.CID[i] != 0 {
			return false
		}
	}
	for i := range r.DEK {
		if r.DEK[i] != 0 {
			return false
		}
	}
	return true
}

func ParseRef(x []byte) (ret Ref, _ error) {
	err := ret.UnmarshalBinary(x)
	return ret, err
}

func AppendRef(out []byte, x Ref) []byte {
	data, _ := x.MarshalBinary()
	return append(out, data...)
}

func Equal(a, b Ref) bool {
	return bytes.Equal(a.CID[:], b.CID[:]) && hmac.Equal(a.DEK[:], b.DEK[:])
}

// Copy copies the data at ref from src to dst.
func Copy(ctx context.Context, src cadata.Getter, dst cadata.Store, ref *Ref) error {
	defer metrics.AddInt(ctx, "blob_copy", 1, "blobs")
	return cadata.Copy(ctx, dst, src, ref.CID)
}
