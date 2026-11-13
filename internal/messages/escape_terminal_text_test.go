package messages

import "testing"

func TestEscapeTerminalText(t *testing.T) {
	for input, want := range map[string]string{
		"plain text":           "plain text",
		"two\nlines\tand tab":  "two\nlines\tand tab",
		"\x1b[31mred\x1b[0m":   `\x1b[31mred\x1b[0m`,
		"bell\a and \r return": `bell\x07 and \x0d return`,
		"del\x7f":              `del\x7f`,
		"c1 \u009b csi":        `c1 \u009b csi`,
		"bidi \u202e reversed": `bidi \u202e reversed`,
		"caf� and émoji 🙂":     "caf� and émoji 🙂",
	} {
		if got := EscapeTerminalText(input); got != want {
			t.Errorf("%q: %q, want %q", input, got, want)
		}
	}
}
