package groups

import "testing"

func TestParseResetTarget(t *testing.T) {
	for text, want := range map[string]ResetTarget{
		"earliest": {Kind: "earliest"}, "latest": {Kind: "latest"},
		"2026-11-12T04:08:00Z": {Kind: "timestamp", Timestamp: 1794456480000}, "1794456480000": {Kind: "timestamp", Timestamp: 1794456480000},
	} {
		if got, err := ParseResetTarget(text); err != nil || got != want {
			t.Errorf("%s: %+v %v", text, got, err)
		}
	}
	for _, bad := range []string{"", "now", "-1", "Latest"} {
		if _, err := ParseResetTarget(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
