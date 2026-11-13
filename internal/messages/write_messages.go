package messages

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// WriteMessages writes one block per message: position and time, key, headers and the value in the chosen format.
// Every piece of message content goes through EscapeTerminalText.
func WriteMessages(w io.Writer, messages []Message, format string) error {
	if len(messages) == 0 {
		_, err := fmt.Fprintln(w, "No messages.")
		return err
	}
	for i, message := range messages {
		if i > 0 {
			fmt.Fprintln(w)
		}
		offset := message.OffsetExact
		if offset == "" {
			offset = fmt.Sprint(message.Offset)
		}
		fmt.Fprintf(w, "partition %d  offset %s  %s\n", message.Partition, offset, message.Timestamp.UTC().Format(time.RFC3339Nano))
		key := "(null)"
		if message.Key != nil {
			key, _ = FormatValue(message.Key, "text")
		}
		fmt.Fprintf(w, "key: %s\n", EscapeTerminalText(key))
		for _, header := range message.Headers {
			value := "(null)"
			if header.Value != nil {
				value, _ = FormatValue(header.Value, "text")
			}
			fmt.Fprintf(w, "header %s: %s\n", EscapeTerminalText(header.Key), EscapeTerminalText(value))
		}
		if message.Value == nil {
			fmt.Fprintln(w, "value: (null)")
			continue
		}
		value, invalidJSON := FormatValue(message.Value, format)
		label := "value:"
		if invalidJSON {
			label = "value (not valid JSON, shown as text):"
		}
		indented := "  " + strings.ReplaceAll(EscapeTerminalText(value), "\n", "\n  ")
		if _, err := fmt.Fprintf(w, "%s\n%s\n", label, indented); err != nil {
			return err
		}
	}
	return nil
}
