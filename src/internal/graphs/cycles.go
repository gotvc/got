package graphs

func FindCycle[V comparable](sources []V, downstream func(out []V, from V) []V) []V {
	visited := make(map[V]bool)
	path := make(map[V]bool)
	cycle := make([]V, 0)

	for _, source := range sources {
		if findCycle(source, downstream, visited, path, &cycle) {
			return cycle
		}
	}

	return nil
}

func findCycle[V comparable](node V, downstream func(out []V, from V) []V, visited map[V]bool, path map[V]bool, cycle *[]V) bool {
	visited[node] = true
	path[node] = true

	neighbors := downstream(nil, node)
	for _, neighbor := range neighbors {
		if !visited[neighbor] {
			if findCycle(neighbor, downstream, visited, path, cycle) {
				*cycle = append(*cycle, neighbor)
				return true
			}
		} else if path[neighbor] {
			*cycle = append(*cycle, neighbor)
			return true
		}
	}

	path[node] = false
	return false
}
