package merklelog

import (
	"context"
	"fmt"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/stores"
)

type CID = blobcache.CID

type Pos = uint64

type State struct {
	// Levels are the rightmost nodes of a binary merkle tree.
	// New entries are always added to level 0, and if there is already a value at that level
	// then it is hashed with the new value to produce a new node, which is then inserted at level 1.
	// This process is repeated until there is no more space at the current level, and a new level is created.
	Levels []CID
}

func Parse(data []byte) (State, error) {
	var s State
	if err := s.Unmarshal(data); err != nil {
		return State{}, err
	}
	return s, nil
}

func (s State) Marshal(out []byte) []byte {
	for _, cid := range s.Levels {
		out = append(out, cid[:]...)
	}
	return out
}

func (s *State) Unmarshal(data []byte) error {
	if len(data)%32 != 0 {
		return fmt.Errorf("merklelog: invalid state, must be multiple of 32 bytes")
	}
	s.Levels = make([]blobcache.CID, len(data)/32)
	for i := 0; i < len(s.Levels); i++ {
		s.Levels[i] = blobcache.CID(data[i*32 : i*32+32])
	}
	return nil
}

// Len returns the number of CIDs included in the state.
func (s State) Len() (ret Pos) {
	for i, cid := range s.Levels {
		if cid != (blobcache.CID{}) {
			ret += (1 << i)
		}
	}
	return ret
}

// Append appends a new CID to the state, it updates the state in place.
func (s *State) Append(ctx context.Context, rws stores.RW, x blobcache.CID) error {
	for i := 0; ; i++ {
		if len(s.Levels) <= i {
			s.Levels = append(s.Levels, blobcache.CID{})
		}
		if s.Levels[i] == (blobcache.CID{}) {
			s.Levels[i] = x
			return nil
		}
		left := s.Levels[i]
		right := x
		x2, err := rws.Post(ctx, slices.Concat(left[:], right[:]))
		if err != nil {
			return err
		}
		s.Levels[i] = blobcache.CID{}
		x = x2
	}
}

// Get retrieves the CID at index i in the log.
func Get(ctx context.Context, rs stores.Reading, s State, i Pos) (blobcache.CID, error) {
	// Check if the position is within bounds
	if i >= s.Len() {
		return blobcache.CID{}, fmt.Errorf("merklelog: position %d out of bounds (length %d)", i, s.Len())
	}

	// Find the level and position within that level where our target resides
	// We need to traverse down the tree to find the leaf at position i

	// Start from the highest level and work our way down
	totalSoFar := Pos(0)

	// Find which "complete" sections of the tree contain our target position
	for level := len(s.Levels) - 1; level >= 0; level-- {
		if s.Levels[level] == (blobcache.CID{}) {
			continue
		}

		levelSize := Pos(1 << level) // 2^level

		if i >= totalSoFar && i < totalSoFar+levelSize {
			// Our target is in this level's subtree
			posInLevel := i - totalSoFar
			return s.getFromSubtree(ctx, rs, s.Levels[level], level, posInLevel)
		}

		totalSoFar += levelSize
	}

	return blobcache.CID{}, fmt.Errorf("merklelog: failed to find position %d", i)
}

// getFromSubtree recursively traverses a subtree to find the element at the given position
func (s State) getFromSubtree(ctx context.Context, rs stores.Reading, root blobcache.CID, level int, pos Pos) (blobcache.CID, error) {
	// If we're at level 0, this is a leaf node - return it directly
	if level == 0 {
		if pos == 0 {
			return root, nil
		}
		return blobcache.CID{}, fmt.Errorf("merklelog: invalid position %d at level 0", pos)
	}

	// For internal nodes, we need to traverse down
	// Get the left and right children
	left, right, err := getNode(ctx, rs, root)
	if err != nil {
		return blobcache.CID{}, err
	}

	// The left subtree contains 2^(level-1) elements
	leftSubtreeSize := Pos(1 << (level - 1))

	if pos < leftSubtreeSize {
		// Target is in the left subtree
		return s.getFromSubtree(ctx, rs, left, level-1, pos)
	} else {
		// Target is in the right subtree
		return s.getFromSubtree(ctx, rs, right, level-1, pos-leftSubtreeSize)
	}
}

func getNode(ctx context.Context, rs stores.Reading, x blobcache.CID) (left, right blobcache.CID, _ error) {
	buf := make([]byte, 32*2)
	n, err := rs.Get(ctx, x, buf)
	if err != nil {
		return blobcache.CID{}, blobcache.CID{}, err
	}
	if n != len(buf) {
		return blobcache.CID{}, blobcache.CID{}, fmt.Errorf("merklelog: invalid node size")
	}
	return blobcache.CID(buf[:32]), blobcache.CID(buf[32:]), nil
}

// Includes returns (true, nil) if a references all of the data in b.
// Includes returns (false, nil) if a cannot reference all of b's data.
// Includes returns (false, err) if an error occurs.
func Includes(ctx context.Context, rs stores.Reading, a, b State) (bool, error) {
	// If a has fewer elements than b, it cannot include all of b's data
	if a.Len() < b.Len() {
		return false, nil
	}

	// TODO: implement a more efficient algorithm.
	// For now just get every element of b and check that it is also in a.
	for i := Pos(0); i < b.Len(); i++ {
		bElem, err := Get(ctx, rs, b, i)
		if err != nil {
			return false, err
		}
		aElem, err := Get(ctx, rs, a, i)
		if err != nil {
			return false, err
		}
		if aElem != bElem {
			return false, nil
		}
	}
	return true, nil
}
