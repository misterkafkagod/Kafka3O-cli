package groups

import "cmp"

// CompareGroups orders groups by id within one cluster, as the UI's list does.
func CompareGroups(a, b ConsumerGroup) int {
	return cmp.Compare(a.ID, b.ID)
}
