package topics

import (
	"slices"
	"testing"
)

func TestCompareClusterTopics(t *testing.T) {
	topics := []Topic{{Cluster: "a", Name: "z"}, {Cluster: "b", Name: "a"}, {Cluster: "a", Name: "m"}}
	slices.SortFunc(topics, CompareClusterTopics(map[string]int{"b": 0, "a": 1}))
	if got := names(topics); got[0] != "b/a" || got[1] != "a/m" || got[2] != "a/z" {
		t.Fatalf("%v", got)
	}
}
