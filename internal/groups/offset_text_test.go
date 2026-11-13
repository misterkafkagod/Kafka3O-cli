package groups

import "testing"

func TestOffsetText(t *testing.T) {
	if offsetText(lag("42")) != "42" || offsetText(nil) != "unknown" {
		t.Fatal("offsetText")
	}
}
