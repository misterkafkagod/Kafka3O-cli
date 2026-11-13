package messages

import (
	"fmt"
	"strings"
	"unicode"
)

// EscapeTerminalText shows control characters (other than line breaks and tabs) as \xNN or \u escapes, so message
// content cannot move the cursor, change colours or send terminal escape sequences.
func EscapeTerminalText(text string) string {
	var out strings.Builder
	for _, r := range text {
		switch {
		case r == '\n' || r == '\t':
			out.WriteRune(r)
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(&out, `\x%02x`, r)
		case unicode.IsControl(r) || r == ' ' || r == ' ' || unicode.Is(unicode.Bidi_Control, r):
			fmt.Fprintf(&out, `\u%04x`, r)
		default:
			out.WriteRune(r)
		}
	}
	return out.String()
}
