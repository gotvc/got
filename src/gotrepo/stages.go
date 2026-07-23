package gotrepo

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gotrepo/internal/reposchema"
	"github.com/gotvc/got/src/internal/volumes"
)

// BeginStagingTx begins a new transaction for the staging area for the given WorkingCopy
// It is up to the caller to commit or abort the transaction.
func (r *Repo) BeginStagingTx(ctx context.Context, wcid WorkingCopyID, modify bool) (volumes.Tx, error) {
	if wcid == (WorkingCopyID{}) {
		return nil, fmt.Errorf("working copy id cannot be 0")
	}
	h, dek, err := r.repoc.StagingArea(ctx, r.rootVol, wcid)
	if err != nil {
		return nil, err
	}
	var vol volumes.Volume = &volumes.Blobcache{Service: r.bc, Handle: *h}
	vol = volumes.NewChaCha20Poly1305(vol, (*[32]byte)(dek))
	return vol.BeginTx(ctx, blobcache.TxParams{Modify: modify})
}

// GCStage begins a new GC transaction for the staging area.
func (r *Repo) GCStage(ctx context.Context, wcid WorkingCopyID) (volumes.Tx, error) {
	h, dek, err := r.repoc.StagingArea(ctx, r.rootVol, wcid)
	if err != nil {
		return nil, err
	}
	var vol volumes.Volume = &volumes.Blobcache{Service: r.bc, Handle: *h}
	vol = volumes.NewChaCha20Poly1305(vol, (*[32]byte)(dek))
	return vol.BeginTx(ctx, blobcache.TxParams{
		Modify:  true,
		GCBlobs: true,
	})
}

type WorkingCopyID = reposchema.StageID

func NewWorkingCopyID() WorkingCopyID {
	return reposchema.NewStageID()
}
