package gotgrpc

import (
	"context"

	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
	"go.brendoncarroll.net/state/cadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	MaxBlobSize = gotfs.DefaultMaxBlobSize
	MaxCellSize = 1 << 16
)

func Hash(x []byte) cadata.ID {
	return gdat.Hash(x)
}

// WithHeaders adds headers to all requests made by the dialer
func WithHeaders(headers map[string]string) grpc.DialOption {
	md := metadata.New(headers)
	incp := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	return grpc.WithUnaryInterceptor(incp)
}
