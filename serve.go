package got

import (
	"context"

	"github.com/brendoncarroll/go-p2p/s/peerswarm"
)

// Server serves cells, and blobs to the network.
func Serve(ctx context.Context, r *Repo, s peerswarm.AskSwarm) error {
	return nil
}
