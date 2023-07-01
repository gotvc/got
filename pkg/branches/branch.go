package branches

import (
	"context"
	"fmt"
	"strings"

	"github.com/brendoncarroll/go-tai64"
	"github.com/gotvc/got/pkg/gdat"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotvc"
	"golang.org/x/exp/slices"
)

type Info struct {
	Mode        Mode         `json:"mode"`
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

func (i Info) AsConfig() Config {
	return Config{Mode: i.Mode, Salt: i.Salt, Annotations: i.Annotations}
}

// Config is non-volume, user-modifiable information associated with a branch.
type Config struct {
	Mode        Mode         `json:"mode"`
	Salt        []byte       `json:"salt"`
	Annotations []Annotation `json:"annotations"`
}

func (c Config) AsInfo() Info {
	return Info{Mode: c.Mode, Salt: c.Salt, Annotations: c.Annotations}
}

func NewConfig(public bool) Config {
	var salt []byte
	if !public {
		salt = make([]byte, 32)
		readRandom(salt)
	}
	return Config{
		Salt: salt,
		Mode: ModeExpand,
	}
}

// Clone returns a deep copy of md
func (c Config) Clone() Config {
	return Config{
		Salt:        slices.Clone(c.Salt),
		Annotations: slices.Clone(c.Annotations),
		Mode:        c.Mode,
	}
}

// Annotation annotates a branch
type Annotation struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

func SortAnnotations(s []Annotation) {
	slices.SortFunc(s, func(a, b Annotation) bool {
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		return a.Value < b.Value
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
func SetHead(ctx context.Context, v Volume, src StoreTriple, snap Snap) error {
	dst := v.StoreTriple()
	return applySnapshot(ctx, v.Cell, func(s *Snap) (*Snap, error) {
		if err := syncStores(ctx, src, dst, snap); err != nil {
			return nil, err
		}
		return &snap, nil
	})
}

// GetHead returns the branch head
func GetHead(ctx context.Context, v Volume) (*Snap, error) {
	return getSnapshot(ctx, v.Cell)
}

// Apply applies fn to branch, any missing data will be pulled from scratch
func Apply(ctx context.Context, v Volume, src StoreTriple, fn func(*Snap) (*Snap, error)) error {
	dst := v.StoreTriple()
	return applySnapshot(ctx, v.Cell, func(x *Snap) (*Snap, error) {
		y, err := fn(x)
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

func History(ctx context.Context, v Volume, vcop *gotvc.Operator, fn func(ref gdat.Ref, snap Snap) error) error {
	snap, err := GetHead(ctx, v)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	ref := vcop.RefFromSnapshot(*snap, v.VCStore)
	if err := fn(ref, *snap); err != nil {
		return err
	}
	return gotvc.ForEach(ctx, v.VCStore, snap.Parents, fn)
}

// NewGotFS creates a new gotfs.Operator suitable for writing to the branch
func NewGotFS(b *Info, opts ...gotfs.Option) *gotfs.Operator {
	opts = append(opts, gotfs.WithSalt(deriveFSSalt(b)))
	fsop := gotfs.NewOperator(opts...)
	return fsop
}

// NewGotVC creates a new gotvc.Operator suitable for writing to the branch
func NewGotVC(b *Info, opts ...gotvc.Option) *gotvc.Operator {
	opts = append(opts, gotvc.WithSalt(deriveVCSalt(b)))
	return gotvc.NewOperator(opts...)
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
