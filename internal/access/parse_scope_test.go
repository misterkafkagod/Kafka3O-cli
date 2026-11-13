package access

import "testing"

func TestParseScope(t *testing.T) {
	for text, want := range map[string]string{"all": "", "kafka-a": "kafka-a", "a.b_c": "a.b_c"} {
		if scope, err := ParseScope(text); err != nil || scope != want {
			t.Errorf("%s: %q %v", text, scope, err)
		}
	}
	for _, bad := range []string{"", "kafka a", "ALL!"} {
		if _, err := ParseScope(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
