package branches

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/tai64"
	"go.inet256.org/inet256/src/inet256"
	"golang.org/x/exp/slices"
)

// Info is the metadata associated with a branch.
type Info struct {
	// Salt is a 32-byte salt used to derive the cryptographic keys for the branch.
	Salt Salt `json:"salt"`
	// Annotations are arbitrary metadata associated with the branch.
	Annotations []Annotation `json:"annotations"`
	// CreatedAt is the time the branch was created.
	CreatedAt tai64.TAI64 `json:"created_at"`
}

func (i Info) Clone() Info {
	i2 := i
	i2.Annotations = slices.Clone(i2.Annotations)
	return i2
}

func (i Info) AsConfig() Params {
	return Params{Salt: i.Salt, Annotations: i.Annotations}
}

// Salt is a 32-byte salt
type Salt [32]byte

func (s Salt) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(s[:])), nil
}

func (s *Salt) UnmarshalText(data []byte) error {
	_, err := hex.Decode(s[:], data)
	return err
}

func (s *Salt) String() string {
	return hex.EncodeToString(s[:])
}

// Params is non-volume, user-modifiable information associated with a branch.
type Params struct {
	Salt        Salt         `json:"salt"`
	Annotations []Annotation `json:"annotations"`
}

func (c Params) AsInfo() Info {
	return Info{Salt: c.Salt, Annotations: c.Annotations}
}

func NewConfig(public bool) Params {
	var salt Salt
	if !public {
		readRandom(salt[:])
	}
	return Params{
		Salt: salt,
	}
}

// Clone returns a deep copy of md
func (c Params) Clone() Params {
	return Params{
		Salt:        c.Salt,
		Annotations: slices.Clone(c.Annotations),
	}
}

// Annotation annotates a branch
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

// Branch associates metadata with a Volume.
type Branch struct {
	Volume Volume
	Info   Info

	gotvc *gotvc.Machine
	gotfs *gotfs.Machine
}

func (b *Branch) init() {
	if b.gotvc == nil {
		b.gotvc = newGotVC(&b.Info)
	}
	if b.gotfs == nil {
		b.gotfs = newGotFS(&b.Info)
	}
}

func (b *Branch) GotFS() *gotfs.Machine {
	b.init()
	return b.gotfs
}

func (b *Branch) GotVC() *gotvc.Machine {
	b.init()
	return b.gotvc
}

func (b *Branch) AsParams() Params {
	return Params{
		Salt:        b.Info.Salt,
		Annotations: b.Info.Annotations,
	}
}

func (b *Branch) GetHead(ctx context.Context) (*Snap, Tx, error) {
	return getSnapshot(ctx, b.Volume)
}

// SetHead forcibly sets the head of the branch.
func (b *Branch) SetHead(ctx context.Context, src stores.Reading, snap Snap) error {
	return applySnapshot(ctx, b.gotvc, b.gotfs, b.Volume, func(dst stores.RW, x *Snap) (*Snap, error) {
		if err := syncStores(ctx, b.gotvc, b.gotfs, src, dst, snap); err != nil {
			return nil, err
		}
		return &snap, nil
	})
}

// ModifyCtx is the context passed to the modify function.
type ModifyCtx struct {
	Store stores.RW
	Head  *Snap
	GotVC *gotvc.Machine
	GotFS *gotfs.Machine
}

func (b *Branch) Modify(ctx context.Context, src stores.Reading, fn func(mctx ModifyCtx) (*Snap, error)) error {
	b.init()
	return applySnapshot(ctx, b.gotvc, b.gotfs, b.Volume, func(dst stores.RW, x *Snap) (*Snap, error) {
		y, err := fn(ModifyCtx{
			Store: dst,
			Head:  x,
			GotVC: b.gotvc,
			GotFS: b.gotfs,
		})
		if err != nil {
			return nil, err
		}
		if y != nil {
			if err := syncStores(ctx, b.gotvc, b.gotfs, src, dst, *y); err != nil {
				return nil, err
			}
		}
		return y, nil
	})
}

func (b *Branch) History(ctx context.Context, fn func(ref gdat.Ref, snap Snap) error) error {
	b.init()
	snap, tx, err := b.GetHead(ctx)
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

func (b *Branch) ViewFS(ctx context.Context, fn func(mach *gotfs.Machine, stores stores.Reading, root gotfs.Root) error) error {
	b.init()
	snap, tx, err := b.GetHead(ctx)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	return fn(b.gotfs, tx, snap.Payload.Root)
}

// NewGotFS creates a new gotfs.Machine suitable for writing to the branch
func newGotFS(b *Info, opts ...gotfs.Option) *gotfs.Machine {
	opts = append(opts, gotfs.WithSalt(deriveFSSalt(b)))
	fsag := gotfs.NewMachine(opts...)
	return fsag
}

// NewGotVC creates a new gotvc.Machine suitable for writing to the branch
func newGotVC(b *Info, opts ...gotvc.Option) *gotvc.Machine {
	opts = append(opts, gotvc.WithSalt(deriveVCSalt(b)))
	return gotvc.NewMachine(opts...)
}

func deriveFSSalt(b *Info) *[32]byte {
	var out [32]byte
	gdat.DeriveKey(out[:], (*[32]byte)(&b.Salt), []byte("gotfs"))
	return &out
}

func deriveVCSalt(b *Info) *[32]byte {
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
