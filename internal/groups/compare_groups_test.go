package groups

import (
	"slices"
	"testing"
)

func TestCompareGroups(t *testing.T) {
	groups := []ConsumerGroup{{ID: "b"}, {ID: "a"}, {ID: "c"}}
	slices.SortFunc(groups, CompareGroups)
	if groups[0].ID != "a" || groups[2].ID != "c" {
		t.Fatalf("%v", groups)
	}
}
