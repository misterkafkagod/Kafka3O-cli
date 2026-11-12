package console

import "testing"

func TestOriginOf(t *testing.T) {
	for input, want := range map[string][2]string{
		"http://localhost:5173":         {"http://localhost:5173", "http://localhost:5173"},
		"http://localhost:5173/":        {"http://localhost:5173", "http://localhost:5173"},
		" https://console.example/ui/ ": {"https://console.example/ui", "https://console.example"},
	} {
		base, origin, err := OriginOf(input)
		if err != nil || base != want[0] || origin != want[1] {
			t.Errorf("%q: %q %q %v", input, base, origin, err)
		}
	}
	for _, input := range []string{"", "localhost:5173", "ftp://x", "http://", "http://u:p@x", "http://x?a=1", "http://x#f"} {
		if _, _, err := OriginOf(input); err == nil {
			t.Errorf("%q accepted", input)
		}
	}
}
