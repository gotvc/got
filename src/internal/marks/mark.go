package marks

import (
	"crypto/rand"
	"errors"
	"strings"

	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotvc"
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

var errReadOnly = errors.New("marks: read-only transaction")
