package groups

import "testing"

func TestStatusLabel(t *testing.T) {
	for status, want := range map[string]string{"ok": "Available", "no_commit": "No committed offset", "out_of_range": "Commit outside log range",
		"unavailable": "Unavailable", "new": "Unknown status"} {
		if got := StatusLabel(status); got != want {
			t.Errorf("%s: %s", status, got)
		}
	}
}
