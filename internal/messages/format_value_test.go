package messages

import "testing"

func TestFormatValueMatchesTheUI(t *testing.T) {
	for _, tt := range []struct {
		value, format, want string
		invalid             bool
	}{
		{"check two", "text", "check two", false},
		{`{"a":1,"b":[true,null]}`, "json", "{\n  \"a\": 1,\n  \"b\": [\n    true,\n    null\n  ]\n}", false},
		{"not json", "json", "not json", true},
		{"check two", "base64", "Y2hlY2sgdHdv", false},
		{"caf\xe9", "text", "caf�", false},
		{"", "text", "", false},
	} {
		got, invalid := FormatValue([]byte(tt.value), tt.format)
		if got != tt.want || invalid != tt.invalid {
			t.Errorf("%q as %s: %q %t, want %q %t", tt.value, tt.format, got, invalid, tt.want, tt.invalid)
		}
	}
}
