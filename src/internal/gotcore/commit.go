package gotcore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotfsvm"
	"github.com/gotvc/got/src/gotvc"
	"github.com/gotvc/got/src/internal/sbe"
	"github.com/gotvc/got/src/internal/stores"
	"go.brendoncarroll.net/exp/slices2"
	"go.brendoncarroll.net/tai64"
	"go.inet256.org/inet256/src/inet256"
)

// Commit is a commitment to a filesystem commit, ancestor Commits, and additional metadata.
type Commit = gotvc.Vertex[Payload]

// Payload is the thing being committed to.
type Payload struct {
	// Snap is the commit of the filesystem.
	Snap  gotfs.Root
	Notes []byte
}

func ParsePayload(data []byte) (Payload, error) {
	var ret Payload
	if err := ret.Unmarshal(data); err != nil {
		return ret, err
	}
	return ret, nil
}

func (p Payload) Marshal(out []byte) []byte {
	out = p.Snap.Marshal(out)
	out = sbe.AppendLP(out, p.Notes)
	return out
}

func (p *Payload) Unmarshal(data []byte) error {
	rootData, data, err := sbe.ReadN(data, gotfs.RootSize)
	if err != nil {
		return err
	}
	root, err := gotfs.ParseRoot(rootData)
	if err != nil {
		return err
	}
	p.Snap = *root
	auxData, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	p.Notes = auxData
	return nil
}

// GetCommit reads a commit from the store.
func GetCommit(ctx context.Context, s stores.RO, ref gdat.Ref) (Commit, error) {
	vcmach := gotvc.NewMachine(gotvc.Params[Payload]{Parse: ParsePayload})
	return vcmach.GetVertex(ctx, s, ref)
}

type CommitParams struct {
	Committer   inet256.ID
	CommittedAt tai64.TAI64
	Base        []Commit
	Snap        gotfs.Root
	Notes       CommitNotes
}

// CreateCommit creates a new Commit in the store.
func CreateCommit(ctx context.Context, vcmach *VCMach, srw stores.RW, copa CommitParams) (Commit, error) {
	if copa.CommittedAt == 0 {
		copa.CommittedAt = tai64.Now().TAI64()
	}
	if copa.Committer.IsZero() {
		return Commit{}, fmt.Errorf("cannot commit")
	}
	if copa.Notes.Authors == nil {
		copa.Notes.Authors = append(copa.Notes.Authors, copa.Committer)
	}
	if copa.Notes.AuthoredAt == 0 {
		copa.Notes.AuthoredAt = copa.CommittedAt
	}
	notes, err := json.Marshal(copa.Notes)
	if err != nil {
		panic(err)
	}
	return vcmach.NewVertex(ctx, srw, gotvc.VertexParams[Payload]{
		Parents:   copa.Base,
		CreatedAt: copa.CommittedAt,
		Creator:   copa.Committer,
		Payload: Payload{
			Snap:  copa.Snap,
			Notes: notes,
		},
	})
}

// PostCommit write a commit to the store.
func PostCommit(ctx context.Context, vcmach *VCMach, srw stores.WO, comm Commit) (gdat.Ref, error) {
	return vcmach.PostVertex(ctx, srw, comm)
}

// Apply applies a function to a root to create a new Root.
func Apply(ctx context.Context, fsmach *gotfs.Machine, ss gotfs.RW, fn gotfsvm.Function, ins []gotfs.Root) (gotfs.Root, error) {
	if len(ins) == 0 {
		base, err := fsmach.NewEmpty(ctx, ss.Metadata, 0o755)
		if err != nil {
			return gotfs.Root{}, err
		}
		ins = append(ins, *base)
	}
	vm := gotfsvm.New(fsmach)
	ins2 := slices2.Map(ins, func(x gotfs.Root) gotfsvm.Input {
		return gotfsvm.Input{
			Stores: ss.RO(),
			Root:   x,
		}
	})
	return vm.Apply(ctx, ss, fn, ins2)
}
