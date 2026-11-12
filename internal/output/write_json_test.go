package output

import (
	"bytes"
	"testing"
)

func TestWriteJSON(t *testing.T) {
	var out bytes.Buffer
	if err := WriteJSON(&out, map[string]any{"id": "kafka-a", "brokers": 1}); err != nil {
		t.Fatal(err)
	}
	if out.String() != "{\n  \"brokers\": 1,\n  \"id\": \"kafka-a\"\n}\n" {
		t.Fatalf("%q", out.String())
	}
}
