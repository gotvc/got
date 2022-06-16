package gotgrpc

import (
	"context"

	"github.com/inet256/inet256/pkg/inet256"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// WithHeaders adds headers to all requests made by the dialer
func WithHeaders(headers map[string]string) grpc.DialOption {
	md := metadata.New(headers)
	incp := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	return grpc.WithUnaryInterceptor(incp)
}

// WithClientCreds configures to the dialer to authenticate to the server using the private key
func WithClientCreds(privateKey inet256.PrivateKey) grpc.DialOption {
	return grpc.WithTransportCredentials(NewClientCreds(privateKey))
}
