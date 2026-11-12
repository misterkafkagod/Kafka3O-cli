package cli

import "testing"

func TestOrUnknown(t *testing.T) {
	value := "42"
	if orUnknown(&value) != "42" || orUnknown(nil) != "unknown" {
		t.Fatal("orUnknown")
	}
}
