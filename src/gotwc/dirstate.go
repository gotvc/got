package gotwc

import (
	"context"
	"encoding/json"

	"github.com/gotvc/got/src/gotwc/internal/porting"
	"github.com/gotvc/got/src/internal/sqlutil"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/kv"
)

// dirState tracks the state of the working directory.
// dirState implements porting.DirState
type dirState struct {
	conn     *sqlutil.Conn
	saltHash [32]byte
}

func newDirState(conn *sqlutil.Conn, saltHash [32]byte) *dirState {
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
