package marks

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/tai64"
	"go.inet256.org/inet256/src/inet256"
	"golang.org/x/exp/slices"
)

type (
	FSMach = gotfs.Machine
	VCMach = gotvc.Machine[Payload]
)

// Info is the metadata associated with a Mark.
type Info struct {
	// Config holds the all the datastructure parameters
	Config DSConfig `json:"config"`
	// Annotations are arbitrary metadata associated with the mark.
	Annotations []Annotation `json:"annotations"`

	// CreatedAt is the time the mark was created.
	CreatedAt tai64.TAI64 `json:"created_at"`
}

func (i Info) Clone() Info {
	i2 := i
	i2.Annotations = slices.Clone(i2.Annotations)
	return i2
}

func (i Info) AsMetadata() Metadata {
	return Metadata{Config: i.Config, Annotations: i.Annotations}
}

// Metadata is non-volume, user-modifiable information associated with a mark.
type Metadata struct {
	Config      DSConfig     `json:"config"`
	Annotations []Annotation `json:"annotations"`
}

func (c Metadata) AsInfo() Info {
	return Info{Config: c.Config, Annotations: c.Annotations}
}

func DefaultConfig(public bool) DSConfig {
	var salt Salt
	if !public {
		readRandom(salt[:])
	}
	return DSConfig{
		Salt: salt,
		GotFS: FSConfig{
			Data: ChunkingConfig{CD: &Chunking_CDConfig{
				MeanSize: gotfs.DefaultMeanBlobSizeData,
				MaxSize:  gotfs.DefaultMaxBlobSize,
			}},
			Metadata: Chunking_CDConfig{
				MeanSize: gotfs.DefaultMeanBlobSizeMetadata,
				MaxSize:  gotfs.DefaultMaxBlobSize,
			},
		},
	}
}

// Clone returns a deep copy of md
func (c Metadata) Clone() Metadata {
	return Metadata{
		Config:      c.Config,
		Annotations: slices.Clone(c.Annotations),
	}
}

// Annotation annotates a mark
type Annotation struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

func SortAnnotations(s []Annotation) {
	slices.SortFunc(s, func(a, b Annotation) int {
		if a.Key != b.Key {
			return strings.Compare(a.Key, b.Key)
		}
		return strings.Compare(a.Value, b.Value)
	})
}

func GetAnnotation(as []Annotation, key string) (ret []Annotation) {
	key = strings.ToLower(key)
	for _, a := range as {
		if strings.ToLower(a.Key) == key {
			ret = append(ret, a)
		}
	}
	return ret
}

// Mark associates metadata with a Volume.
type Mark struct {
	Volume Volume
	Info   Info

	gotvc *VCMach
	gotfs *FSMach
}

func (b *Mark) init() {
	if b.gotvc == nil {
		b.gotvc = newGotVC(&b.Info.Config)
	}
	if b.gotfs == nil {
		b.gotfs = newGotFS(&b.Info.Config)
	}
}

func (b *Mark) GotFS() *gotfs.Machine {
	b.init()
	return b.gotfs
}

func (b *Mark) GotVC() *VCMach {
	b.init()
	return b.gotvc
}

func (m *Mark) Config() DSConfig {
	return m.Info.Config
}

func (b *Mark) AsMetadata() Metadata {
	return Metadata{
		Config:      b.Config(),
		Annotations: b.Info.Annotations,
	}
}

func (b *Mark) GetTarget(ctx context.Context) (*Snap, Tx, error) {
	return getSnapshot(ctx, b.Volume)
}

// SetTarget forcibly sets the root of the mark.
func (b *Mark) SetTarget(ctx context.Context, src stores.Reading, snap Snap) error {
	return applySnapshot(ctx, b.gotvc, b.gotfs, b.Volume, func(dst stores.RW, x *Snap) (*Snap, error) {
		if err := syncStores(ctx, b.gotvc, b.gotfs, src, dst, snap); err != nil {
			return nil, err
		}
		return &snap, nil
	})
}

// ModifyCtx is the context passed to the modify function.
type ModifyCtx struct {
	VC    *VCMach
	FS    *FSMach
	Store stores.RW
	Root  *Snap
}

// Sync syncs a snapshot into the store
func (mctx *ModifyCtx) Sync(ctx context.Context, srcs [3]stores.Reading, root Snap) error {
	return mctx.VC.Sync(ctx, srcs[2], mctx.Store, root, func(payload Payload) error {
		return mctx.FS.Sync(ctx,
			[2]stores.Reading{srcs[0], srcs[1]},
			[2]stores.Writing{mctx.Store, mctx.Store},
			payload.Root,
		)
	})
}

func (b *Mark) Modify(ctx context.Context, fn func(mctx ModifyCtx) (*Snap, error)) error {
	b.init()
	return applySnapshot(ctx, b.gotvc, b.gotfs, b.Volume, func(dst stores.RW, x *Snap) (*Snap, error) {
		y, err := fn(ModifyCtx{
			Store: dst,
			Root:  x,
			VC:    b.gotvc,
			FS:    b.gotfs,
		})
		if err != nil {
			return nil, err
		}
		if y != nil {
			if err := syncStores(ctx, b.gotvc, b.gotfs, dst, dst, *y); err != nil {
				return nil, err
			}
		}
		return y, nil
	})
}

func (b *Mark) History(ctx context.Context, fn func(ref gdat.Ref, snap Snap) error) error {
	b.init()
	snap, tx, err := b.GetTarget(ctx)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if snap == nil {
		return nil
	}
	ref := b.gotvc.RefFromSnapshot(*snap)
	if err := fn(ref, *snap); err != nil {
		return err
	}
	return b.gotvc.ForEach(ctx, tx, snap.Parents, fn)
}

func (b *Mark) ViewFS(ctx context.Context, fn func(mach *gotfs.Machine, stores stores.Reading, root gotfs.Root) error) error {
	b.init()
	snap, tx, err := b.GetTarget(ctx)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if snap == nil {
		return fmt.Errorf("mark has no target")
	}
	return fn(b.gotfs, tx, snap.Payload.Root)
}

// NewGotFS creates a new gotfs.Machine suitable for writing to the mark
func newGotFS(b *DSConfig, opts ...gotfs.Option) *gotfs.Machine {
	opts = append(opts, gotfs.WithSalt(deriveFSSalt(b)))
	fsag := gotfs.NewMachine(opts...)
	return fsag
}

// NewGotVC creates a new gotvc.Machine suitable for writing to the mark
func newGotVC(b *DSConfig, opts ...gotvc.Option[Payload]) *VCMach {
	opts = append(opts, gotvc.WithSalt[Payload](deriveVCSalt(b)))
	return gotvc.NewMachine(ParsePayload, opts...)
}

func deriveFSSalt(b *DSConfig) *[32]byte {
	var out [32]byte
	gdat.DeriveKey(out[:], (*[32]byte)(&b.Salt), []byte("gotfs"))
	return &out
}

func deriveVCSalt(b *DSConfig) *[32]byte {
	var out [32]byte
	gdat.DeriveKey(out[:], (*[32]byte)(&b.Salt), []byte("gotvc"))
	return &out
}

// SnapInfo holds additional information about a snapshot.
// This is stored as json in the snapshot.
type SnapInfo struct {
	AuthoredAt tai64.TAI64  `json:"authored_at"`
	Authors    []inet256.ID `json:"authors"`

	Message string `json:"message"`
}

func readRandom(out []byte) {
	if _, err := rand.Read(out); err != nil {
		panic(err)
	}
}
