package messages

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestWriteMessages(t *testing.T) {
	at := time.Date(2026, 11, 12, 4, 8, 52, 928000000, time.UTC)
	messages := []Message{
		{Partition: 0, Offset: 61, OffsetExact: "61", Timestamp: at, Key: []byte("r-b"), Value: []byte(`{"check":2}`),
			Headers: []Header{{Key: "check", Value: []byte("restore-1112")}, {Key: "empty"}}},
		{Partition: 3, Offset: 21, OffsetExact: "21", Timestamp: at, Value: []byte("\x1b]0;owned\x07plain")},
		{Partition: 1, Offset: 2, OffsetExact: "2", Timestamp: at, Key: []byte("k"), Value: []byte("not json")},
	}
	var out bytes.Buffer
	if err := WriteMessages(&out, messages, "json"); err != nil {
		t.Fatal(err)
	}
	want := "partition 0  offset 61  2026-11-12T04:08:52.928Z\nkey: r-b\nheader check: restore-1112\nheader empty: (null)\nvalue:\n  {\n    \"check\": 2\n  }\n\n" +
		"partition 3  offset 21  2026-11-12T04:08:52.928Z\nkey: (null)\nvalue (not valid JSON, shown as text):\n  \\x1b]0;owned\\x07plain\n\n" +
		"partition 1  offset 2  2026-11-12T04:08:52.928Z\nkey: k\nvalue (not valid JSON, shown as text):\n  not json\n"
	if out.String() != want {
		t.Fatalf("%q\nwant\n%q", out.String(), want)
	}
	if strings.Contains(out.String(), "\x1b") {
		t.Fatal("a raw escape sequence was printed")
	}
}

func TestWriteMessagesEmptyAndNullValue(t *testing.T) {
	var out bytes.Buffer
	_ = WriteMessages(&out, nil, "text")
	if out.String() != "No messages.\n" {
		t.Fatalf("%q", out.String())
	}
	out.Reset()
	_ = WriteMessages(&out, []Message{{Partition: 0, Offset: 1, Timestamp: time.Unix(0, 0)}}, "text")
	if !strings.Contains(out.String(), "offset 1 ") || !strings.Contains(out.String(), "value: (null)") {
		t.Fatalf("%q", out.String())
	}
}
