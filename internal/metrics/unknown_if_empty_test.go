package metrics

import "testing"

func TestUnknownIfEmpty(t *testing.T) {
	if unknownIfEmpty("") != "unknown" || unknownIfEmpty("0") != "0" {
		t.Fatal("unknownIfEmpty")
	}
}
