package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

func text(value string) *string { return &value }

func TestWriteTopicView(t *testing.T) {
	written := time.Date(2026, 11, 12, 4, 10, 0, 0, time.UTC)
	view := topics.TopicView{Cluster: "kafka-a", Name: "kafka3o-m10-verify",
		Partitions: []topics.TopicPartition{{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1}, StartOffset: 0, EndOffset: 9007199254740993}},
		Config:     []topics.TopicConfig{{Name: "cleanup.policy", Value: "delete"}, {Name: "secret.thing", Value: "x", Sensitive: true}},
		Groups: topics.TopicGroups{Unresolved: 1, Groups: []topics.ConsumerGroup{{ID: "readers", State: "Stable", Partitions: []topics.GroupPartition{
			{Topic: "kafka3o-m10-verify", Partition: 0, CommittedOffset: text("9007199254740990"), EndOffset: text("9007199254740993"), Lag: text("3")},
			{Topic: "kafka3o-m10-verify", Partition: 1}}}}},
		Backup: topics.TopicBackup{Backup: &topics.BackupSnapshot{State: "RUNNING", Location: "s3://kafka3o-backups/kafka-a/kafka3o-m10-verify/",
			Storage:  &topics.BackupStorage{Objects: 12, StoredBytes: 2400, LastWrite: &written},
			Settings: &topics.BackupSettings{StartFrom: "earliest", Compression: "gzip"},
			Restore:  &topics.BackupRestore{State: "RUNNING", Restored: 1, Total: 4}}},
	}
	var out bytes.Buffer
	if err := WriteTopicView(&out, view); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Topic kafka3o-m10-verify on kafka-a",
		"0          1       1,2       1    0      9007199254740993",
		"cleanup.policy  delete", "secret.thing    (sensitive, hidden)",
		"readers  Stable  0          9007199254740990  9007199254740993  3",
		"readers  Stable  1          unknown           unknown           unknown",
		"1 consumer group(s) could not be described",
		"State:      RUNNING", "Stored:     2400 bytes in 12 objects", "Settings:   from the oldest message, gzip, no record limit",
		"Restore:    restoring 1 of 4 records",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "x\n") {
		t.Error("a sensitive value was printed")
	}
}

func TestWriteTopicViewUnavailableSections(t *testing.T) {
	for backup, want := range map[topics.TopicBackup]string{
		{NotConfigured: true}:                   "Backups are not configured for this cluster.",
		{Error: "Kafka Connect is unreachable"}: "Unavailable: Kafka Connect is unreachable",
		{}:                                      "No backup for this topic.",
	} {
		var out bytes.Buffer
		_ = WriteTopicView(&out, topics.TopicView{Cluster: "c", Name: "t", Groups: topics.TopicGroups{Error: "list groups: timeout"}, Backup: backup})
		if !strings.Contains(out.String(), want) || !strings.Contains(out.String(), "Consumer groups\nUnavailable: list groups: timeout") {
			t.Errorf("missing %q in\n%s", want, out.String())
		}
	}
	var out bytes.Buffer
	_ = WriteTopicView(&out, topics.TopicView{Cluster: "c", Name: "t"})
	if !strings.Contains(out.String(), "No consumer groups read this topic.") {
		t.Errorf("no groups:\n%s", out.String())
	}
}
