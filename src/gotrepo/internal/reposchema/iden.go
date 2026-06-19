package reposchema

import (
	"bytes"
	"context"
	"fmt"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotkv"
	"github.com/gotvc/got/src/gotorg"
	"go.inet256.org/inet256/src/inet256"
)

func (c *Client) PostIdentity(ctx context.Context, repoVol blobcache.OID, idp gotorg.IdenPrivate) (inet256.ID, error) {
	rootH, err := c.rootHandle(ctx, repoVol)
	if err != nil {
		return inet256.ID{}, err
	}
	txn, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: true})
	if err != nil {
		return inet256.ID{}, err
	}
	defer txn.Abort(ctx)

	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return inet256.ID{}, err
	}
	id := idp.GetID()
	data, err := MarshalIden(idp)
	if err != nil {
		return inet256.ID{}, err
	}
	if existingRefData, err := c.gotkv.Get(ctx, txn, root, idenKey(id)); err == nil {
		existingRef, err := gdat.ParseRef(existingRefData)
		if err != nil {
			return inet256.ID{}, err
		}
		var existingData []byte
		if err := c.dmach.GetF(ctx, txn, existingRef, func(x []byte) error {
			existingData = append(existingData, x...)
			return nil
		}); err != nil {
			return inet256.ID{}, err
		}
		if !bytes.Equal(existingData, data) {
			return inet256.ID{}, fmt.Errorf("identity already exists with different value: %v", id)
		}
		return id, nil
	} else if !gotkv.IsErrKeyNotFound(err) {
		return inet256.ID{}, err
	}
	ref, err := c.dmach.Post(ctx, txn, data)
	if err != nil {
		return inet256.ID{}, err
	}
	root, err = c.gotkv.Put(ctx, txn, root, idenKey(id), gdat.AppendRef(nil, ref))
	if err != nil {
		return inet256.ID{}, err
	}
	if err := txn.Save(ctx, root.Marshal(nil)); err != nil {
		return inet256.ID{}, err
	}
	if err := txn.Commit(ctx); err != nil {
		return inet256.ID{}, err
	}
	return id, nil
}

func (c *Client) GetIdentity(ctx context.Context, repoVol blobcache.OID, id inet256.ID) (*gotorg.IdenPrivate, error) {
	rootH, err := c.rootHandle(ctx, repoVol)
	if err != nil {
		return nil, err
	}
	txn, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: false})
	if err != nil {
		return nil, err
	}
	defer txn.Abort(ctx)

	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return nil, err
	}
	refData, err := c.gotkv.Get(ctx, txn, root, idenKey(id))
	if err != nil {
		return nil, err
	}
	ref, err := gdat.ParseRef(refData)
	if err != nil {
		return nil, err
	}
	var ret *gotorg.IdenPrivate
	if err := c.dmach.GetF(ctx, txn, ref, func(data []byte) error {
		x, err := ParseIden(data)
		if err != nil {
			return err
		}
		ret = x
		return nil
	}); err != nil {
		return nil, err
	}
	if ret.GetID() != id {
		return nil, fmt.Errorf("identity id mismatch HAVE=%v WANT=%v", ret.GetID(), id)
	}
	return ret, nil
}

func idenKey(id inet256.ID) []byte {
	ret := append([]byte{}, []byte("iden/")...)
	ret = append(ret, id[:]...)
	return ret
}

func MarshalIden(idp gotorg.IdenPrivate) ([]byte, error) {
	sigPrivData, err := pki.MarshalPrivateKey(nil, idp.SigPrivateKey)
	if err != nil {
		return nil, err
	}
	kemPrivData := gotorg.MarshalKEMPrivateKey(nil, gotorg.KEM_MLKEM1024, idp.KEMPrivateKey)
	ret := fmt.Appendf(nil, "SIG %x\nKEM %x\n", sigPrivData, kemPrivData)
	return ret, nil
}

func ParseIden(data []byte) (*gotorg.IdenPrivate, error) {
	x := string(data)
	var sigPrivData []byte
	var kemPrivData []byte
	_, err := fmt.Sscanf(x, "SIG %x\nKEM %x\n", &sigPrivData, &kemPrivData)
	if err != nil {
		return nil, err
	}
	sigPriv, err := pki.ParsePrivateKey(sigPrivData)
	if err != nil {
		return nil, err
	}
	kemPriv, err := gotorg.ParseKEMPrivateKey(kemPrivData)
	if err != nil {
		return nil, err
	}
	return &gotorg.IdenPrivate{
		SigPrivateKey: sigPriv,
		KEMPrivateKey: kemPriv,
	}, nil
}

var pki = gotorg.PKI()
