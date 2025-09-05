package gotled

import (
	"encoding/json"
	"testing"

	"github.com/gotvc/got/src/internal/merklelog"
	"github.com/stretchr/testify/require"
)

func TestMarshal(t *testing.T) {
	type state = autoMarshal[int]
	type proof = autoMarshal[int]
	x := Root[state, proof]{
		History: merklelog.State{Levels: []merklelog.CID{}},
		State:   autoMarshal[int]{X: 1},
		Proof:   autoMarshal[int]{X: 2},
	}
	parseState := func(data []byte) (state, error) {
		var x state
		if err := x.Unmarshal(data); err != nil {
			return state{}, err
		}
		return x, nil
	}
	parseProof := func(data []byte) (proof, error) {
		var x proof
		if err := x.Unmarshal(data); err != nil {
			return proof{}, err
		}
		return x, nil
	}
	y, err := Parse(x.Marshal(nil), parseState, parseProof)
	require.NoError(t, err)
	require.Equal(t, x, y)
}

type autoMarshal[T any] struct {
	X T
}

func (s autoMarshal[T]) Marshal(out []byte) []byte {
	data, err := json.Marshal(s.X)
	if err != nil {
		panic(err)
	}
	out = append(out, data...)
	return out
}

func (s *autoMarshal[T]) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &s.X)
}
