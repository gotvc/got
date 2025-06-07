package gotgrpc

import (
	"go.brendoncarroll.net/exp/slices2"
	"go.brendoncarroll.net/tai64"

	"github.com/gotvc/got/pkg/branches"
)

func (bi *BranchInfo) ToInfo() *branches.Info {
	if bi == nil {
		return nil
	}
	createdAt, _ := tai64.Parse(bi.CreatedAt)
	return &branches.Info{
		Salt: bi.Salt,
		Mode: branches.Mode(bi.Mode),
		Annotations: slices2.Map(bi.Annotations, func(x *Annotation) branches.Annotation {
			return x.ToAnnotation()
		}),

		CreatedAt: createdAt,
	}
}

func ProtoBranchInfo(x *branches.Info) *BranchInfo {
	return &BranchInfo{
		Salt:        x.Salt,
		Mode:        Mode(x.Mode),
		Annotations: slices2.Map(x.Annotations, ProtoAnnotation),

		CreatedAt: x.CreatedAt.Marshal(),
	}
}

func (a *Annotation) ToAnnotation() branches.Annotation {
	if a == nil {
		return branches.Annotation{}
	}
	return branches.Annotation{Key: a.Key, Value: a.Value}
}

func ProtoAnnotation(x branches.Annotation) *Annotation {
	return &Annotation{Key: x.Key, Value: x.Value}
}
