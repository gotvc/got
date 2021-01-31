package gotkv

import (
	"context"

	"github.com/blobcache/blobcache/pkg/bccrypto"
	"github.com/brendoncarroll/got/pkg/cadata"
	"github.com/brendoncarroll/got/pkg/gotkv/gkvproto"
	capnp "zombiezen.com/go/capnproto2"
)

type Ref = gkvproto.Ref

func newRef() Ref {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		panic(err)
	}
	ref, err := gkvproto.NewRef(seg)
	if err != nil {
		panic(err)
	}
	return ref
}

func PostRaw(ctx context.Context, s Store, data []byte) (*Ref, error) {
	kf := bccrypto.SaltedConvergent(nil)
	id, dek, err := bccrypto.Post(ctx, s, kf, data)
	if err != nil {
		return nil, err
	}
	ref := newRef()
	ref.SetCid(id[:])
	ref.SetDek(dek[:])
	return &ref, nil
}

func GetRawF(ctx context.Context, s Store, ref Ref, fn func(data []byte) error) error {
	cidData, err := ref.Cid()
	if err != nil {
		return err
	}
	cid := cadata.IDFromBytes(cidData)
	dekData, err := ref.Dek()
	if err != nil {
		return err
	}
	dek := bccrypto.DEK(cadata.IDFromBytes(dekData))
	return bccrypto.GetF(ctx, s, dek, cid, fn)
}
