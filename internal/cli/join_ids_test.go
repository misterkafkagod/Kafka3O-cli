package cli

import "testing"

func TestJoinIDs(t *testing.T) {
	if got := joinIDs([]int32{1, 2, 3}); got != "1,2,3" {
		t.Fatal(got)
	}
	if got := joinIDs(nil); got != "" {
		t.Fatal(got)
	}
}
