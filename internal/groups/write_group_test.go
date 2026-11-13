package groups

import (
	"bytes"
	"strings"
	"testing"
)

func TestWriteGroup(t *testing.T) {
	instance := "static-1"
	group := ConsumerGroup{Cluster: "kafka-a", ID: "readers", Protocol: "classic", State: "Stable",
		Members: []Member{{ID: "m-1", ClientID: "c-1", ClientHost: "/10.0.0.1", InstanceID: &instance}},
		Partitions: []Partition{
			{Topic: "orders", Partition: 0, CommittedOffset: lag("9007199254740990"), StartOffset: lag("0"), EndOffset: lag("9007199254740993"), Lag: lag("3"), Status: "ok"},
			{Topic: "payments", Partition: 1, Status: "no_commit"},
			{Topic: "orders", Partition: 1, CommittedOffset: lag("2"), StartOffset: lag("5"), EndOffset: lag("9"), Status: "out_of_range"},
		}}
	var out bytes.Buffer
	if err := WriteGroup(&out, group); err != nil {
		t.Fatal(err)
	}
	squeeze := func(text string) string { return strings.Join(strings.Fields(text), " ") }
	for _, want := range []string{"Group readers on kafka-a", "State: Stable (classic protocol)", "m-1 c-1 /10.0.0.1 static-1",
		"Offsets for orders", "0 9007199254740990 0 9007199254740993 3 Available", "1 2 5 9 unknown Commit outside log range",
		"Offsets for payments", "1 unknown unknown unknown unknown No committed offset"} {
		if !strings.Contains(squeeze(out.String()), want) {
			t.Errorf("missing %q in\n%s", want, out.String())
		}
	}
	if strings.Index(out.String(), "Offsets for orders") > strings.Index(out.String(), "Offsets for payments") {
		t.Error("topics are not in the Gateway's order")
	}
}

func TestWriteGroupWithoutMembersOrOffsets(t *testing.T) {
	var out bytes.Buffer
	_ = WriteGroup(&out, ConsumerGroup{Cluster: "c", ID: "g", State: "Dead", Error: "COORDINATOR_NOT_AVAILABLE"})
	for _, want := range []string{"Unavailable: COORDINATOR_NOT_AVAILABLE", "No members.", "No committed offsets."} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in\n%s", want, out.String())
		}
	}
	if (ErrGroupNotFound{Cluster: "kafka-a", Group: "nope"}).Error() != "group nope was not found on kafka-a" {
		t.Error("ErrGroupNotFound text")
	}
}
