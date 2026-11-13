package metrics

import "testing"

func TestEscapeCSVField(t *testing.T) {
	for field, want := range map[string]string{"plain": "plain", "a,b": `"a,b"`, `say "hi"`: `"say ""hi"""`, "two\nlines": "\"two\nlines\"", "": ""} {
		if got := EscapeCSVField(field); got != want {
			t.Errorf("%q: %q, want %q", field, got, want)
		}
	}
}
