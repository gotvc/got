// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.14.0
// source: got.proto

package gotgrpc

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// GotSpaceClient is the client API for GotSpace service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type GotSpaceClient interface {
	CreateBranch(ctx context.Context, in *CreateBranchReq, opts ...grpc.CallOption) (*BranchInfo, error)
	GetBranch(ctx context.Context, in *GetBranchReq, opts ...grpc.CallOption) (*BranchInfo, error)
	DeleteBranch(ctx context.Context, in *DeleteBranchReq, opts ...grpc.CallOption) (*DeleteBranchRes, error)
	ListBranch(ctx context.Context, in *ListBranchReq, opts ...grpc.CallOption) (*ListBranchRes, error)
	SetBranch(ctx context.Context, in *SetBranchReq, opts ...grpc.CallOption) (*BranchInfo, error)
	PostBlob(ctx context.Context, in *PostBlobReq, opts ...grpc.CallOption) (*PostBlobRes, error)
	GetBlob(ctx context.Context, in *GetBlobReq, opts ...grpc.CallOption) (*GetBlobRes, error)
	DeleteBlob(ctx context.Context, in *DeleteBlobReq, opts ...grpc.CallOption) (*DeleteBlobRes, error)
	AddBlob(ctx context.Context, in *AddBlobReq, opts ...grpc.CallOption) (*AddBlobRes, error)
	ListBlob(ctx context.Context, in *ListBlobReq, opts ...grpc.CallOption) (*ListBlobRes, error)
	ReadCell(ctx context.Context, in *ReadCellReq, opts ...grpc.CallOption) (*ReadCellRes, error)
	CASCell(ctx context.Context, in *CASCellReq, opts ...grpc.CallOption) (*CASCellRes, error)
}

type gotSpaceClient struct {
	cc grpc.ClientConnInterface
}

func NewGotSpaceClient(cc grpc.ClientConnInterface) GotSpaceClient {
	return &gotSpaceClient{cc}
}

func (c *gotSpaceClient) CreateBranch(ctx context.Context, in *CreateBranchReq, opts ...grpc.CallOption) (*BranchInfo, error) {
	out := new(BranchInfo)
	err := c.cc.Invoke(ctx, "/got.GotSpace/CreateBranch", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gotSpaceClient) GetBranch(ctx context.Context, in *GetBranchReq, opts ...grpc.CallOption) (*BranchInfo, error) {
	out := new(BranchInfo)
	err := c.cc.Invoke(ctx, "/got.GotSpace/GetBranch", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gotSpaceClient) DeleteBranch(ctx context.Context, in *DeleteBranchReq, opts ...grpc.CallOption) (*DeleteBranchRes, error) {
	out := new(DeleteBranchRes)
	err := c.cc.Invoke(ctx, "/got.GotSpace/DeleteBranch", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gotSpaceClient) ListBranch(ctx context.Context, in *ListBranchReq, opts ...grpc.CallOption) (*ListBranchRes, error) {
	out := new(ListBranchRes)
	err := c.cc.Invoke(ctx, "/got.GotSpace/ListBranch", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gotSpaceClient) SetBranch(ctx context.Context, in *SetBranchReq, opts ...grpc.CallOption) (*BranchInfo, error) {
	out := new(BranchInfo)
	err := c.cc.Invoke(ctx, "/got.GotSpace/SetBranch", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gotSpaceClient) PostBlob(ctx context.Context, in *PostBlobReq, opts ...grpc.CallOption) (*PostBlobRes, error) {
	out := new(PostBlobRes)
	err := c.cc.Invoke(ctx, "/got.GotSpace/PostBlob", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gotSpaceClient) GetBlob(ctx context.Context, in *GetBlobReq, opts ...grpc.CallOption) (*GetBlobRes, error) {
	out := new(GetBlobRes)
	err := c.cc.Invoke(ctx, "/got.GotSpace/GetBlob", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gotSpaceClient) DeleteBlob(ctx context.Context, in *DeleteBlobReq, opts ...grpc.CallOption) (*DeleteBlobRes, error) {
	out := new(DeleteBlobRes)
	err := c.cc.Invoke(ctx, "/got.GotSpace/DeleteBlob", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gotSpaceClient) AddBlob(ctx context.Context, in *AddBlobReq, opts ...grpc.CallOption) (*AddBlobRes, error) {
	out := new(AddBlobRes)
	err := c.cc.Invoke(ctx, "/got.GotSpace/AddBlob", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gotSpaceClient) ListBlob(ctx context.Context, in *ListBlobReq, opts ...grpc.CallOption) (*ListBlobRes, error) {
	out := new(ListBlobRes)
	err := c.cc.Invoke(ctx, "/got.GotSpace/ListBlob", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gotSpaceClient) ReadCell(ctx context.Context, in *ReadCellReq, opts ...grpc.CallOption) (*ReadCellRes, error) {
	out := new(ReadCellRes)
	err := c.cc.Invoke(ctx, "/got.GotSpace/ReadCell", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gotSpaceClient) CASCell(ctx context.Context, in *CASCellReq, opts ...grpc.CallOption) (*CASCellRes, error) {
	out := new(CASCellRes)
	err := c.cc.Invoke(ctx, "/got.GotSpace/CASCell", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GotSpaceServer is the server API for GotSpace service.
// All implementations must embed UnimplementedGotSpaceServer
// for forward compatibility
type GotSpaceServer interface {
	CreateBranch(context.Context, *CreateBranchReq) (*BranchInfo, error)
	GetBranch(context.Context, *GetBranchReq) (*BranchInfo, error)
	DeleteBranch(context.Context, *DeleteBranchReq) (*DeleteBranchRes, error)
	ListBranch(context.Context, *ListBranchReq) (*ListBranchRes, error)
	SetBranch(context.Context, *SetBranchReq) (*BranchInfo, error)
	PostBlob(context.Context, *PostBlobReq) (*PostBlobRes, error)
	GetBlob(context.Context, *GetBlobReq) (*GetBlobRes, error)
	DeleteBlob(context.Context, *DeleteBlobReq) (*DeleteBlobRes, error)
	AddBlob(context.Context, *AddBlobReq) (*AddBlobRes, error)
	ListBlob(context.Context, *ListBlobReq) (*ListBlobRes, error)
	ReadCell(context.Context, *ReadCellReq) (*ReadCellRes, error)
	CASCell(context.Context, *CASCellReq) (*CASCellRes, error)
	mustEmbedUnimplementedGotSpaceServer()
}

// UnimplementedGotSpaceServer must be embedded to have forward compatible implementations.
type UnimplementedGotSpaceServer struct {
}

func (UnimplementedGotSpaceServer) CreateBranch(context.Context, *CreateBranchReq) (*BranchInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateBranch not implemented")
}
func (UnimplementedGotSpaceServer) GetBranch(context.Context, *GetBranchReq) (*BranchInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBranch not implemented")
}
func (UnimplementedGotSpaceServer) DeleteBranch(context.Context, *DeleteBranchReq) (*DeleteBranchRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteBranch not implemented")
}
func (UnimplementedGotSpaceServer) ListBranch(context.Context, *ListBranchReq) (*ListBranchRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListBranch not implemented")
}
func (UnimplementedGotSpaceServer) SetBranch(context.Context, *SetBranchReq) (*BranchInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetBranch not implemented")
}
func (UnimplementedGotSpaceServer) PostBlob(context.Context, *PostBlobReq) (*PostBlobRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PostBlob not implemented")
}
func (UnimplementedGotSpaceServer) GetBlob(context.Context, *GetBlobReq) (*GetBlobRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBlob not implemented")
}
func (UnimplementedGotSpaceServer) DeleteBlob(context.Context, *DeleteBlobReq) (*DeleteBlobRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteBlob not implemented")
}
func (UnimplementedGotSpaceServer) AddBlob(context.Context, *AddBlobReq) (*AddBlobRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddBlob not implemented")
}
func (UnimplementedGotSpaceServer) ListBlob(context.Context, *ListBlobReq) (*ListBlobRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListBlob not implemented")
}
func (UnimplementedGotSpaceServer) ReadCell(context.Context, *ReadCellReq) (*ReadCellRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadCell not implemented")
}
func (UnimplementedGotSpaceServer) CASCell(context.Context, *CASCellReq) (*CASCellRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CASCell not implemented")
}
func (UnimplementedGotSpaceServer) mustEmbedUnimplementedGotSpaceServer() {}

// UnsafeGotSpaceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to GotSpaceServer will
// result in compilation errors.
type UnsafeGotSpaceServer interface {
	mustEmbedUnimplementedGotSpaceServer()
}

func RegisterGotSpaceServer(s grpc.ServiceRegistrar, srv GotSpaceServer) {
	s.RegisterService(&GotSpace_ServiceDesc, srv)
}

func _GotSpace_CreateBranch_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateBranchReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).CreateBranch(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/CreateBranch",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).CreateBranch(ctx, req.(*CreateBranchReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _GotSpace_GetBranch_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetBranchReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).GetBranch(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/GetBranch",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).GetBranch(ctx, req.(*GetBranchReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _GotSpace_DeleteBranch_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteBranchReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).DeleteBranch(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/DeleteBranch",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).DeleteBranch(ctx, req.(*DeleteBranchReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _GotSpace_ListBranch_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListBranchReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).ListBranch(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/ListBranch",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).ListBranch(ctx, req.(*ListBranchReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _GotSpace_SetBranch_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetBranchReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).SetBranch(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/SetBranch",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).SetBranch(ctx, req.(*SetBranchReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _GotSpace_PostBlob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PostBlobReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).PostBlob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/PostBlob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).PostBlob(ctx, req.(*PostBlobReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _GotSpace_GetBlob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetBlobReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).GetBlob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/GetBlob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).GetBlob(ctx, req.(*GetBlobReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _GotSpace_DeleteBlob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteBlobReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).DeleteBlob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/DeleteBlob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).DeleteBlob(ctx, req.(*DeleteBlobReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _GotSpace_AddBlob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddBlobReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).AddBlob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/AddBlob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).AddBlob(ctx, req.(*AddBlobReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _GotSpace_ListBlob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListBlobReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).ListBlob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/ListBlob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).ListBlob(ctx, req.(*ListBlobReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _GotSpace_ReadCell_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadCellReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).ReadCell(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/ReadCell",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).ReadCell(ctx, req.(*ReadCellReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _GotSpace_CASCell_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CASCellReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GotSpaceServer).CASCell(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/got.GotSpace/CASCell",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GotSpaceServer).CASCell(ctx, req.(*CASCellReq))
	}
	return interceptor(ctx, in, info, handler)
}

// GotSpace_ServiceDesc is the grpc.ServiceDesc for GotSpace service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var GotSpace_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "got.GotSpace",
	HandlerType: (*GotSpaceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateBranch",
			Handler:    _GotSpace_CreateBranch_Handler,
		},
		{
			MethodName: "GetBranch",
			Handler:    _GotSpace_GetBranch_Handler,
		},
		{
			MethodName: "DeleteBranch",
			Handler:    _GotSpace_DeleteBranch_Handler,
		},
		{
			MethodName: "ListBranch",
			Handler:    _GotSpace_ListBranch_Handler,
		},
		{
			MethodName: "SetBranch",
			Handler:    _GotSpace_SetBranch_Handler,
		},
		{
			MethodName: "PostBlob",
			Handler:    _GotSpace_PostBlob_Handler,
		},
		{
			MethodName: "GetBlob",
			Handler:    _GotSpace_GetBlob_Handler,
		},
		{
			MethodName: "DeleteBlob",
			Handler:    _GotSpace_DeleteBlob_Handler,
		},
		{
			MethodName: "AddBlob",
			Handler:    _GotSpace_AddBlob_Handler,
		},
		{
			MethodName: "ListBlob",
			Handler:    _GotSpace_ListBlob_Handler,
		},
		{
			MethodName: "ReadCell",
			Handler:    _GotSpace_ReadCell_Handler,
		},
		{
			MethodName: "CASCell",
			Handler:    _GotSpace_CASCell_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "got.proto",
}
