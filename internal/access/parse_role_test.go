package access

import "testing"

func TestParseRole(t *testing.T) {
	for text, want := range map[string]string{"VIEWER": "VIEWER", "operator": "OPERATOR", "Admin": "ADMIN", "none": "", "NONE": ""} {
		if role, err := ParseRole(text); err != nil || role != want {
			t.Errorf("%s: %q %v", text, role, err)
		}
	}
	for _, bad := range []string{"", "owner", "admins"} {
		if _, err := ParseRole(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
