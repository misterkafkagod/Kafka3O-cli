package messages

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
)

// FormatValue is a port of the UI's formatMessageValue: "text" decodes UTF-8 (invalid bytes become U+FFFD, as the
// browser's TextDecoder does), "json" pretty-prints with two spaces and falls back to the text with invalidJSON set,
// "base64" returns the bytes as the Gateway sent them.
func FormatValue(value []byte, format string) (text string, invalidJSON bool) {
	switch format {
	case "base64":
		return base64.StdEncoding.EncodeToString(value), false
	case "json":
		var pretty bytes.Buffer
		decoded := strings.ToValidUTF8(string(value), "�")
		if json.Valid([]byte(decoded)) && json.Indent(&pretty, []byte(decoded), "", "  ") == nil {
			return pretty.String(), false
		}
		return decoded, true
	default:
		return strings.ToValidUTF8(string(value), "�"), false
	}
}
