// Package dgotfs manages deltas and operations that change GotFS filesystems.
package dgotfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"maps"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/internal/stores"
)

// Op is a filesystem operation.
type Op struct {
	Create *OpCreate `json:"create,omitempty"`

	MData    *OpMData    `json:"mdata,omitempty"`
	MModeSet *OpMModeSet `json:"modeset,omitempty"`
	MAttr    *OpMAttr    `json:"mattr,omitempty"`

	Delete *OpDelete `json:"delete,omitempty"`

	RPfix *OpRPfix `json:"rpfix,omitempty"`

	// All is a set of Ops which must all run without conflicts
	All OpSet `json:"all,omitempty"`
	// Prog is a program, a sequence of ops to run one after another.
	Prog OpSeq `json:"prog,omitempty"`
}

func (o Op) Hash() blobcache.CID {
	data, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}
	return gdat.Hash(data)
}

func (o *Op) Code() string {
	switch {
	case o.Create != nil:
		return "CREATE"
	case o.Delete != nil:
		return "DELETE"
	case o.RPfix != nil:
		return "READ_PREFIX"
	default:
		return "UNKNOWN"
	}
}

func (o Op) ShortDesc() string {
	switch {
	case o.Create != nil:
		return o.Create.Path
	default:
		return fmt.Sprintf("(%+v)", o)
	}
}

func (o Op) Print(w io.Writer) {
	fmt.Fprintf(w, "%12s %s", o.Code(), o.ShortDesc())
}

// OpCreate creates a new file (or directory)
// Create provides entirely fresh data, and represents
// the total state of the filesystem.
type OpCreate struct {
	// Path is the path to create the object at
	Path string `json:"path"`
	// Root is a filesystem where the int
	Root gotfs.Root `json:"root"`
}

type OpMData struct {
	Path string     `json:"path"`
	Root gotfs.Root `json:"root"`
}

type OpMModeSet struct {
	Path string      `json:"path"`
	Mode fs.FileMode `json:"mode"`
}

type OpMAttr struct {
	Path string     `json:"path"`
	Root gotfs.Root `json:"root"`
}

type OpDelete string

// OpRPfix marks a region of the filesystem as read
type OpRPfix struct {
	Prefix string `json:"prefix"`
}

// OpSet is a set of operations which must not conflict
type OpSet map[blobcache.CID]Op

// OpSeq is a sequence of operations that must.
type OpSeq []Op

type Machine struct {
	gotfs *gotfs.Machine
}

func New(fsmach *gotfs.Machine) Machine {
	return Machine{gotfs: fsmach}
}

func (m *Machine) Apply(ctx context.Context, ss [2]stores.RW, root gotfs.Root, op Op) (*gotfs.Root, error) {
	_, ms := ss[0], ss[1]
	switch {
	case op.All != nil:
		if err := m.CheckConflict(maps.Values(op.All)); err != nil {
			return nil, err
		}
		for _, op := range op.All {
			root2, err := m.Apply(ctx, ss, root, op)
			if err != nil {
				return nil, err
			}
			root = *root2
		}
	case op.Prog != nil:
		for _, op := range op.Prog {
			root2, err := m.Apply(ctx, ss, root, op)
			if err != nil {
				return nil, err
			}
			root = *root2
		}

	case op.RPfix != nil:
		op := *op.RPfix
		_, err := m.gotfs.GetInfo(ctx, ms, root, op.Prefix)
		if err != nil {
			return nil, err
		}
	case op.Create != nil:
		op := *op.Create
		yes, err := m.gotfs.Exists(ctx, ms, root, op.Path)
		if err != nil {
			return nil, err
		}
		if yes {
			return nil, fmt.Errorf("cannot perform create, file already exists")
		}
		root2, err := m.gotfs.Graft(ctx, ss, root, op.Path, op.Root)
		if err != nil {
			return nil, err
		}
		root = *root2
	case op.Delete != nil:
		op := *op.Delete
		root2, err := m.gotfs.RemoveAll(ctx, ms, root, string(op))
		if err != nil {
			return nil, err
		}
		root = *root2
	}
	return &root, nil
}

func (m *Machine) CheckConflict(ops iter.Seq[Op]) error {
	return nil
}
