package branches

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/tai64"
	"go.inet256.org/inet256/src/inet256"
	"golang.org/x/exp/slices"
)

type Info struct {
	Salt        []byte       `json:"salt"`
	Annotations []Annotation `json:"annotations"`

	CreatedAt tai64.TAI64 `json:"created_at"`
}

func (i Info) Clone() Info {
	i2 := i
	i2.Salt = slices.Clone(i2.Salt)
	i2.Annotations = slices.Clone(i2.Annotations)
	return i2
}

func (i Info) AsConfig() Params {
	return Params{Salt: i.Salt, Annotations: i.Annotations}
}

// Params is non-volume, user-modifiable information associated with a branch.
type Params struct {
	Salt        []byte       `json:"salt"`
	Annotations []Annotation `json:"annotations"`
}

func (c Params) AsInfo() Info {
	return Info{Salt: c.Salt, Annotations: c.Annotations}
}

func NewConfig(public bool) Params {
	var salt []byte
	if !public {
		salt = make([]byte, 32)
		readRandom(salt)
	}
	return Params{
		Salt: salt,
	}
}

// Clone returns a deep copy of md
func (c Params) Clone() Params {
	return Params{
		Salt:        slices.Clone(c.Salt),
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

type Mode uint8

const (
	ModeFrozen = iota
	ModeExpand
	ModeShrink
)

func (m Mode) MarshalText() ([]byte, error) {
	switch m {
	case ModeFrozen:
		return []byte("FROZEN"), nil
	case ModeExpand:
		return []byte("EXPAND"), nil
	case ModeShrink:
		return []byte("SHRINK"), nil
	default:
		return nil, fmt.Errorf("Mode(INVALID, %d)", m)
	}
}

func (m *Mode) UnmarshalText(data []byte) error {
	switch string(data) {
	case "FROZEN":
		*m = ModeFrozen
	case "EXPAND":
		*m = ModeExpand
	case "SHRINK":
		*m = ModeShrink
	default:
		return fmt.Errorf("invalid mode %q", data)
	}
	return nil
}

func (m Mode) String() string {
	switch m {
	case ModeFrozen:
		return "FROZEN"
	case ModeExpand:
		return "EXPAND"
	case ModeShrink:
		return "SHRINK"
	default:
		return fmt.Sprintf("Mode(INVALID, %d)", m)
	}
}

// SetHead forcibly sets the head of the branch.
func SetHead(ctx context.Context, dst Volume, src cadata.Getter, snap Snap) error {
	return applySnapshot(ctx, dst, func(dst stores.RW, x *Snap) (*Snap, error) {
		if err := syncStores(ctx, src, dst, snap); err != nil {
			return nil, err
		}
		return &snap, nil
	})
}

// GetHead returns the branch head
func GetHead(ctx context.Context, v Volume) (*Snap, Tx, error) {
	return getSnapshot(ctx, v)
}

// Apply applies fn to branch, any missing data will be pulled from src.
func Apply(ctx context.Context, dstVol Volume, src stores.Reading, fn func(stores.RW, *Snap) (*Snap, error)) error {
	return applySnapshot(ctx, dstVol, func(dst stores.RW, x *Snap) (*Snap, error) {
		y, err := fn(dst, x)
		if err != nil {
			return nil, err
		}
		if y != nil {
			if err := syncStores(ctx, src, dst, *y); err != nil {
				return nil, err
			}
		}
		return y, nil
	})
}

func History(ctx context.Context, vcag *gotvc.Machine, v Volume, fn func(ref gdat.Ref, snap Snap) error) error {
	snap, tx, err := GetHead(ctx, v)
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	if snap == nil {
		return nil
	}
	ref := vcag.RefFromSnapshot(*snap, tx)
	if err := fn(ref, *snap); err != nil {
		return err
	}
	return vcag.ForEach(ctx, tx, snap.Parents, fn)
}

// NewGotFS creates a new gotfs.Machine suitable for writing to the branch
func NewGotFS(b *Info, opts ...gotfs.Option) *gotfs.Machine {
	opts = append(opts, gotfs.WithSalt(deriveFSSalt(b)))
	fsag := gotfs.NewMachine(opts...)
	return fsag
}

// NewGotVC creates a new gotvc.Machine suitable for writing to the branch
func NewGotVC(b *Info, opts ...gotvc.Option) *gotvc.Machine {
	opts = append(opts, gotvc.WithSalt(deriveVCSalt(b)))
	return gotvc.NewMachine(opts...)
}

func deriveFSSalt(b *Info) *[32]byte {
	var out [32]byte
	gdat.DeriveKey(out[:], saltFromBytes(b.Salt), []byte("gotfs"))
	return &out
}

func deriveVCSalt(b *Info) *[32]byte {
	var out [32]byte
	gdat.DeriveKey(out[:], saltFromBytes(b.Salt), []byte("gotvc"))
	return &out
}

func saltFromBytes(x []byte) *[32]byte {
	var salt [32]byte
	copy(salt[:], x)
	return &salt
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
