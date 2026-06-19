package reposchema

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotkv"
)

var configKey = []byte("config")

func (c *Client) GetConfig(ctx context.Context, repoVol blobcache.OID) (json.RawMessage, error) {
	rootH, err := c.rootHandle(ctx, repoVol)
	if err != nil {
		return nil, err
	}
	txn, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: true})
	if err != nil {
		return nil, err
	}
	defer txn.Abort(ctx)

	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return nil, err
	}
	cfg, err := c.gotkv.Get(ctx, txn, root, configKey)
	if err != nil {
		if gotkv.IsErrKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return append(json.RawMessage(nil), cfg...), nil
}

func (c *Client) EditConfig(ctx context.Context, repoVol blobcache.OID, fn func(x json.RawMessage) json.RawMessage) error {
	rootH, err := c.rootHandle(ctx, repoVol)
	if err != nil {
		return err
	}
	txn, err := bcsdk.BeginTx(ctx, c.bcsvc, *rootH, blobcache.TxParams{Modify: true})
	if err != nil {
		return err
	}
	defer txn.Abort(ctx)

	root, err := c.getRoot(ctx, txn)
	if err != nil {
		return err
	}
	var prev json.RawMessage
	if cfg, err := c.gotkv.Get(ctx, txn, root, configKey); err != nil {
		if !gotkv.IsErrKeyNotFound(err) {
			return err
		}
	} else {
		prev = append(json.RawMessage(nil), cfg...)
	}
	next := fn(prev)
	if next == nil {
		root, err = c.gotkv.Delete(ctx, txn, root, configKey)
		if err != nil {
			return err
		}
	} else {
		root, err = c.gotkv.Put(ctx, txn, root, configKey, next)
		if err != nil {
			return err
		}
	}
	if err := txn.Save(ctx, root.Marshal(nil)); err != nil {
		return err
	}
	return txn.Commit(ctx)
}
