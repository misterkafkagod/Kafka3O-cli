package metrics

import "strings"

// EscapeCSVField quotes a field that holds a comma, quote or line break (RFC 4180), like the UI's escapeCsvField.
func EscapeCSVField(field string) string {
	if strings.ContainsAny(field, "\",\r\n") {
		return `"` + strings.ReplaceAll(field, `"`, `""`) + `"`
	}
	return field
}
