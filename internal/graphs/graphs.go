package graphs

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
