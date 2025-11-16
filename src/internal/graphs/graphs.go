package graphs

import "iter"

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

func Dijkstras[V comparable](starting []V, target V, dwn DownstreamFunc[V]) []V {
	p, _ := DijkstrasErr(starting, target, func(v V) iter.Seq2[V, error] {
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

func DijkstrasErr[V comparable](starting []V, target V, dwn DownstreamFuncErr[V]) ([]V, error) {
	return nil, nil
}
