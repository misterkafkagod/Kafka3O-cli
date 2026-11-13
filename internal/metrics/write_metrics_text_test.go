package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestWriteMetricsText(t *testing.T) {
	m := decode(t, base+`,
  "quorum": {"leaderId": 1, "leaderEpoch": 3, "highWatermark": 894587, "replicas": [{"id": 1, "role": "voter", "logEndOffset": 894587, "lag": 0, "lastFetch": "2026-11-13T10:00:00Z"}]},
  "brokers": {"brokers": [{"broker": 1, "leaders": 39, "replicas": 39, "nonPreferredLeaders": 0}]},
  "consumerGroups": {"states": [{"state": "Empty", "count": 1}, {"state": "Stable", "count": 1}], "groups": [
    {"group": "readers", "state": "Stable", "members": 1, "lag": "0", "maxPartitionLag": "0", "unknownPartitions": 0},
    {"group": "probe", "state": "Empty", "members": 0, "lag": "40", "maxPartitionLag": "40", "unknownPartitions": 3}]},
  "connect": {"connectors": [{"state": "RUNNING", "count": 5}], "tasks": [{"state": "FAILED", "count": 1}], "failures": [{"connector": "kafka3o-verify-failing", "task": 0, "error": "ConnectException: no file"}]},
  "backups": {"backups": [{"topic": "orders", "state": "RUNNING", "storageListed": true, "objects": 10, "storedBytes": 1900, "lastWrite": "2026-11-11T03:30:30Z", "restoring": false}]}}`)
	var out bytes.Buffer
	if err := WriteMetricsText(&out, "kafka-a", m); err != nil {
		t.Fatal(err)
	}
	squeeze := func(text string) string { return strings.Join(strings.Fields(text), " ") }
	for _, want := range []string{
		"Metrics for kafka-a, read at 2026-11-10T12:00:00Z",
		"Topics 2, partitions 4, under-replicated 1, below min ISR unknown, offline 0", "Below min ISR is unknown: denied",
		"Leader 1, epoch 3, high watermark 894587", "1        voter  894587   0    2026-11-13T10:00:00Z",
		"1       /data          100          60          7", "2       /data,b        unknown      unknown     3", "3       /broken        Unavailable: KAFKA_STORAGE_ERROR",
		"1       39       39        0",
		"Empty 1, Stable 1", "readers  Stable  1        0", "probe    Empty   0        at least 40 (3 partitions unknown)",
		"connectors running 5, tasks failed 1", "Failed: kafka3o-verify-failing task 0: ConnectException: no file",
		"orders  RUNNING  2026-11-11T03:30:30Z  1900", "orders  7",
	} {
		if !strings.Contains(squeeze(out.String()), squeeze(want)) {
			t.Errorf("missing %q in\n%s", want, out.String())
		}
	}
}

func TestWriteMetricsTextUnavailableSections(t *testing.T) {
	m := decode(t, `{"readAt": "r", "logDirs": {"error": "denied", "dirs": []}, "partitions": {"error": "timeout"}, "largestTopics": {"error": "denied", "topics": []},
	  "endOffsets": {"topics": []}, "quorum": {"error": "describe quorum: CLUSTER_AUTHORIZATION_FAILED", "leaderEpoch": 0, "highWatermark": 0, "replicas": []},
	  "connect": {"notConfigured": true}, "backups": {"error": "kafka connect is unreachable"}, "consumerGroups": {"states": [], "groups": []}}`)
	var out bytes.Buffer
	_ = WriteMetricsText(&out, "kafka-b", m)
	for _, want := range []string{"Partition health\nUnavailable: timeout", "Controller quorum\nUnavailable: describe quorum: CLUSTER_AUTHORIZATION_FAILED",
		"Unavailable: denied", "Kafka Connect is not configured for this cluster.", "Backups\nUnavailable: kafka connect is unreachable", "No consumer groups."} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in\n%s", want, out.String())
		}
	}
}
