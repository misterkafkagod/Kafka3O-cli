package connect

import "cmp"

// CompareTasks orders a connector's tasks by id.
func CompareTasks(a, b Task) int {
	return cmp.Compare(a.ID, b.ID)
}
