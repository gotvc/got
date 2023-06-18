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

// Branch is a Volume plus additional metadata
type Branch struct {
	Volume Volume
	Metadata

	CreatedAt tai64.TAI64 `json:"created_at"`
}

// Metadata is non-volume, user-modifiable information associated with a branch.
type Metadata struct {
	Mode Mode   `json:"mode"`
	Salt []byte `json:"salt"`

	Annotations []Annotation `json:"annotations"`
}

func NewMetadata(public bool) Metadata {
	var salt []byte
	if !public {
		salt = make([]byte, 32)
		readRandom(salt)
	}
	return Metadata{
		Salt: salt,
		Mode: ModeExpand,
	}
}

// Clone returns a deep copy of md
func (md Metadata) Clone() Metadata {
	return Metadata{
		Salt:        slices.Clone(md.Salt),
		Annotations: slices.Clone(md.Annotations),
		Mode:        md.Mode,
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
func SetHead(ctx context.Context, b Branch, src StoreTriple, snap Snap) error {
	dst := b.Volume.StoreTriple()
	return applySnapshot(ctx, b.Volume.Cell, func(s *Snap) (*Snap, error) {
		if err := syncStores(ctx, src, dst, snap); err != nil {
			return nil, err
		}
		return &snap, nil
	})
}

// GetHead returns the branch head
func GetHead(ctx context.Context, b Branch) (*Snap, error) {
	return getSnapshot(ctx, b.Volume.Cell)
}

// Apply applies fn to branch, any missing data will be pulled from scratch
func Apply(ctx context.Context, b Branch, src StoreTriple, fn func(*Snap) (*Snap, error)) error {
	dst := b.Volume.StoreTriple()
	return applySnapshot(ctx, b.Volume.Cell, func(x *Snap) (*Snap, error) {
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

func History(ctx context.Context, b Branch, vcop *gotvc.Operator, fn func(ref gdat.Ref, snap Snap) error) error {
	snap, err := GetHead(ctx, b)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	ref := vcop.RefFromSnapshot(*snap, b.Volume.VCStore)
	if err := fn(ref, *snap); err != nil {
		return err
	}
	return gotvc.ForEach(ctx, b.Volume.VCStore, snap.Parents, fn)
}

// NewGotFS creates a new gotfs.Operator suitable for writing to the branch
func NewGotFS(b *Branch, opts ...gotfs.Option) *gotfs.Operator {
	opts = append(opts, gotfs.WithSalt(deriveFSSalt(b)))
	fsop := gotfs.NewOperator(opts...)
	return fsop
}

// NewGotVC creates a new gotvc.Operator suitable for writing to the branch
func NewGotVC(b *Branch, opts ...gotvc.Option) *gotvc.Operator {
	opts = append(opts, gotvc.WithSalt(deriveVCSalt(b)))
	return gotvc.NewOperator(opts...)
}

func deriveFSSalt(b *Branch) *[32]byte {
	var out [32]byte
	gdat.DeriveKey(out[:], saltFromBytes(b.Salt), []byte("gotfs"))
	return &out
}

func deriveVCSalt(b *Branch) *[32]byte {
	var out [32]byte
	gdat.DeriveKey(out[:], saltFromBytes(b.Salt), []byte("gotvc"))
	return &out
}

func saltFromBytes(x []byte) *[32]byte {
	var salt [32]byte
	copy(salt[:], x)
	return &salt
}
