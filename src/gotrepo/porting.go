package gotrepo

import (
	"context"
	"encoding/json"

	"github.com/gotvc/got/src/internal/dbutil"
	"github.com/gotvc/got/src/internal/porting"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/kv"
)

// DoWithStore runs fn with a store for the desired branch
func (r *Repo) DoWithStore(ctx context.Context, branchName string, fn func(dst stores.RW) error) error {
	return r.modifyStaging(ctx, func(sctx stagingCtx) error {
		return fn(sctx.Store)
	})
}

// dirState tracks the state of the working directory.
type dirState struct {
	conn     *dbutil.Conn
	saltHash [32]byte
}

func newDirState(conn *dbutil.Conn, saltHash [32]byte) *dirState {
	return &dirState{
		conn:     conn,
		saltHash: saltHash,
	}
}

func (c *dirState) Get(ctx context.Context, p string, dst *porting.Entry) error {
	return state.ErrNotFound[string]{Key: p}
}

func (c *dirState) Exists(ctx context.Context, p string) (bool, error) {
	return kv.ExistsUsingList(ctx, c, p)
}

func (c *dirState) Put(ctx context.Context, p string, ent porting.Entry) error {
	_, err := json.Marshal(ent)
	if err != nil {
		return err
	}
	return nil
}

func (c *dirState) Delete(ctx context.Context, p string) error {
	return nil
}

func (c *dirState) List(ctx context.Context, span state.Span[string], ks []string) (int, error) {
	return 0, nil
}

func saltFromBytes(x []byte) *[32]byte {
	var salt [32]byte
	copy(salt[:], x)
	return &salt
}
