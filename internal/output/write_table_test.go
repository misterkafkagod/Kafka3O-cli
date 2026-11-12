package output

import (
	"bytes"
	"testing"
)

func TestWriteTable(t *testing.T) {
	var out bytes.Buffer
	if err := WriteTable(&out, []string{"CLUSTER", "NAME"}, [][]string{{"kafka-a", "Kafka A"}, {"b", "with\ttab\nand line"}}); err != nil {
		t.Fatal(err)
	}
	want := "CLUSTER  NAME\nkafka-a  Kafka A\nb        with tab and line\n"
	if out.String() != want {
		t.Fatalf("%q\nwant %q", out.String(), want)
	}
}
