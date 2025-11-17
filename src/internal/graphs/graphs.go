package graphs

import (
	"iter"
	"slices"

	"go.brendoncarroll.net/exp/heaps"
)

// DownstreamFunc returns an iterator over Vertices immediately downstream of a given Vertex.
type DownstreamFunc[V comparable] = func(V) iter.Seq[V]

type DownstreamFuncErr[V comparable] = func(V) iter.Seq2[V, error]

func CanReach[V comparable](source V, target V, downstream func(out []V, from V) []V) bool {
	visited := make(map[V]bool)
	return canReach(source, target, downstream, visited)
}

func canReach[V comparable](node V, target V, downstream func(out []V, from V) []V, visited map[V]bool) bool {
	visited[node] = true

	if node == target {
		return true
	}
	neighbors := downstream(nil, node)
	for _, neighbor := range neighbors {
		if !visited[neighbor] {
			if canReach(neighbor, target, downstream, visited) {
				return true
			}
		}
	}
	return false
}

func Dijkstras[V comparable](starting []V, goal func(V) bool, dwn DownstreamFunc[V]) []V {
	p, _ := DijkstrasErr(starting, goal, func(v V) iter.Seq2[V, error] {
		return func(yield func(V, error) bool) {
			for v2 := range dwn(v) {
				if !yield(v2, nil) {
					return
				}
			}
		}
	})
	return p
}

func DijkstrasErr[V comparable](starting []V, goal func(V) bool, dwn DownstreamFuncErr[V]) ([]V, error) {
	costs := map[V]int{}
	prevs := map[V]V{}
	queue := heaps.New(func(a, b V) bool {
		_, haveA := costs[a]
		_, haveB := costs[b]
		if !haveA || !haveB {
			panic("vertex in queue without cost")
		}
		return costs[a] < costs[b]
	})
	for _, v := range starting {
		costs[v] = 0
		queue.Push(v)
	}
	var found bool
	var target V
OUTER:
	for queue.Len() > 0 {
		v := queue.Pop()
		if goal(v) {
			found = true
			target = v
			break OUTER
		}
		for v2, err := range dwn(v) {
			if err != nil {
				return nil, err
			}
			newCost := costs[v] + 1
			if oldCost, exists := costs[v2]; !exists || newCost < oldCost {
				costs[v2] = newCost
				prevs[v2] = v
			}
			queue.Push(v2)
		}
	}
	if !found {
		return nil, nil // no path
	}

	ret := []V{target}
	for costs[ret[len(ret)-1]] != 0 {
		v := ret[len(ret)-1]
		v2 := prevs[v]
		ret = append(ret, v2)
	}
	slices.Reverse(ret)
	return ret, nil
}
