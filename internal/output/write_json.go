// Package output writes command results as aligned tables for people or JSON for scripts.
package output

import (
	"encoding/json"
	"io"
)

// WriteJSON writes the value as indented JSON followed by a newline.
func WriteJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
