package gotlob

import (
	"context"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotkv"
)

type Option func(o *Operator)

func WithSalt(salt *[32]byte) Option {
	return func(o *Operator) {
		o.salt = salt
	}
}

// WithMetaCacheSize sets the size of the cache for metadata
func WithMetaCacheSize(n int) Option {
	return func(o *Operator) {
		o.metaCacheSize = n
	}
}

// WithContentCacheSize sets the size of the cache for raw data
func WithContentCacheSize(n int) Option {
	return func(o *Operator) {
		o.rawCacheSize = n
	}
}

type Operator struct {
	maxBlobSize                                 int
	minSizeData, averageSizeData, averageSizeKV int
	salt                                        *[32]byte
	rawCacheSize, metaCacheSize                 int

	rawOp        gdat.Operator
	gotkv        gotkv.Operator
	chunkingSeed *[32]byte
}

func NewOperator(opts ...Option) Operator {
	o := Operator{
		maxBlobSize:     DefaultMaxBlobSize,
		minSizeData:     DefaultMinBlobSizeData,
		averageSizeData: DefaultAverageBlobSizeData,
		averageSizeKV:   DefaultAverageBlobSizeKV,
		salt:            &[32]byte{},
		rawCacheSize:    8,
		metaCacheSize:   16,
	}
	for _, opt := range opts {
		opt(&o)
	}

	// data
	var rawSalt [32]byte
	gdat.DeriveKey(rawSalt[:], o.salt, []byte("raw"))
	o.rawOp = gdat.NewOperator(
		gdat.WithSalt(&rawSalt),
		gdat.WithCacheSize(o.rawCacheSize),
	)
	var chunkingSeed [32]byte
	gdat.DeriveKey(chunkingSeed[:], o.salt, []byte("chunking"))
	o.chunkingSeed = &chunkingSeed

	// metadata
	var metadataSalt [32]byte
	gdat.DeriveKey(metadataSalt[:], o.salt, []byte("gotkv"))
	metaOp := gdat.NewOperator(
		gdat.WithSalt(&metadataSalt),
		gdat.WithCacheSize(o.metaCacheSize),
	)
	var treeSeed [16]byte
	gdat.DeriveKey(treeSeed[:], o.salt, []byte("gotkv-seed"))
	o.gotkv = gotkv.NewOperator(
		o.averageSizeKV,
		o.maxBlobSize,
		gotkv.WithDataOperator(metaOp),
		gotkv.WithSeed(&treeSeed),
	)
	return o
}

func (o *Operator) CreateExtents(ctx context.Context, ms, ds cadata.Store, r io.Reader) ([]*Extent, error) {
	return nil, nil
}

func (op *Operator) post(ctx context.Context, s cadata.Store, data []byte) (*Extent, error) {
	l := len(data)
	for len(data)%64 != 0 {
		data = append(data, 0x00)
	}
	ref, err := op.rawOp.Post(ctx, s, data)
	if err != nil {
		return nil, err
	}
	return &Extent{Offset: 0, Length: uint32(l), Ref: *ref}, nil
}
