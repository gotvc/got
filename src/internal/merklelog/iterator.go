package merklelog

import (
	"context"

	"github.com/gotvc/got/src/internal/stores"
)

type Iterator struct {
	state State
	store stores.Reading
	beg   Pos
	end   Pos
}

func NewIterator(state State, s stores.Reading, beg, end Pos) *Iterator {
	return &Iterator{
		state: state,
		store: s,
		beg:   beg,
		end:   end,
	}
}

func (it *Iterator) Next(ctx context.Context, dst *CID) error {
	// TODO: we should keep more of the tree nodes in memory to avoid re-reading them
	// Get traverses from the root for every call.
	cid, err := Get(ctx, it.store, it.state, it.beg)
	if err != nil {
		return err
	}
	*dst = cid
	it.beg++
	return nil
}
