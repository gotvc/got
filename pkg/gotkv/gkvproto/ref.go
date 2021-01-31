package gkvproto

import (
	"encoding/json"

	"github.com/blobcache/blobcache/pkg/bccrypto"
	"github.com/brendoncarroll/got/pkg/cadata"
	capnp "zombiezen.com/go/capnproto2"
)

type jsonRef struct {
	CID cadata.ID    `json:"cid"`
	DEK bccrypto.DEK `json:"dek"`
}

func (r Ref) MarshalJSON() ([]byte, error) {
	cidData, err := r.Cid()
	if err != nil {
		return nil, err
	}
	dekData, err := r.Dek()
	if err != nil {
		return nil, err
	}
	return json.Marshal(jsonRef{
		CID: cadata.IDFromBytes(cidData),
		DEK: bccrypto.DEK(cadata.IDFromBytes(dekData)),
	})
}

func (r *Ref) UnmarshalJSON(data []byte) error {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return err
	}
	ref, err := NewRef(seg)
	if err != nil {
		return err
	}
	*r = ref

	var x jsonRef
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	if err := r.SetCid(x.CID[:]); err != nil {
		return err
	}
	if err := r.SetDek(x.DEK[:]); err != nil {
		return err
	}
	return nil
}
