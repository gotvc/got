package refs

import (
	"bytes"
	"crypto/hmac"
	"encoding/base64"

	"github.com/blobcache/blobcache/pkg/bccrypto"
	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/pkg/errors"
)

type Ref struct {
	CID cadata.ID
	DEK bccrypto.DEK
}

func (r Ref) MarshalText() ([]byte, error) {
	codec := base64.RawURLEncoding
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
	codec := base64.RawURLEncoding
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

// func (r Ref) String() string {
// 	data, _ := r.MarshalText()
// 	return string(data)
// }

func ParseRef(x []byte) (Ref, error) {
	var ref Ref
	if len(x) != len(ref.CID)+len(ref.DEK) {
		return Ref{}, errors.Errorf("wrong length for ref")
	}
	copy(ref.CID[:], x[:len(ref.CID)])
	copy(ref.DEK[:], x[len(ref.CID):])
	return ref, nil
}

func MarshalRef(x Ref) []byte {
	buf := make([]byte, len(x.CID)+len(x.DEK))
	n := copy(buf, x.CID[:])
	copy(buf[n:], x.DEK[:])
	return buf
}

func Equal(a, b Ref) bool {
	return bytes.Equal(a.CID[:], b.CID[:]) && hmac.Equal(a.DEK[:], b.DEK[:])
}
