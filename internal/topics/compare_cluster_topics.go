package topics

import "cmp"

// CompareClusterTopics orders topics by the cluster's position in the Console's list, then by name.
func CompareClusterTopics(order map[string]int) func(a, b Topic) int {
	return func(a, b Topic) int {
		if c := cmp.Compare(order[a.Cluster], order[b.Cluster]); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	}
}
