package metrics

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func decode(t *testing.T, document string) ClusterMetrics {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader([]byte(document)))
	decoder.UseNumber()
	var m ClusterMetrics
	if err := decoder.Decode(&m); err != nil {
		t.Fatal(err)
	}
	return m
}

// The fixture and the expected lines are the UI's metricsToCsv tests, so both exports stay identical.
const base = `{
  "readAt": "2026-11-10T12:00:00Z",
  "logDirs": {"dirs": [
    {"broker": 1, "dir": "/data", "totalBytes": 100, "usableBytes": 60, "kafkaBytes": 7},
    {"broker": 2, "dir": "/data,b", "totalBytes": null, "usableBytes": null, "kafkaBytes": 3},
    {"broker": 3, "dir": "/broken", "totalBytes": null, "usableBytes": null, "kafkaBytes": 0, "error": "KAFKA_STORAGE_ERROR"}]},
  "partitions": {"topics": 2, "partitions": 4, "underReplicated": 1, "offline": 0, "belowMinIsr": null, "minIsrError": "denied", "unreadableTopics": []},
  "largestTopics": {"topics": [{"topic": "orders", "bytes": 7}]},
  "endOffsets": {"topics": [{"topic": "orders", "endOffset": "9007199254740993"}, {"topic": "failing", "error": "NOT_LEADER"}]}`

func TestMetricsToCSVMatchesTheUI(t *testing.T) {
	got := strings.Split(MetricsToCSV(decode(t, base+"}")), "\r\n")
	want := []string{
		"metric,broker,logDir,topic,name,value",
		"read_at,,,,,2026-11-10T12:00:00Z",
		"log_dir_total_bytes,1,/data,,,100",
		"log_dir_used_bytes,1,/data,,,40",
		"log_dir_usable_bytes,1,/data,,,60",
		"log_dir_kafka_bytes,1,/data,,,7",
		`log_dir_total_bytes,2,"/data,b",,,`,
		`log_dir_used_bytes,2,"/data,b",,,`,
		`log_dir_usable_bytes,2,"/data,b",,,`,
		`log_dir_kafka_bytes,2,"/data,b",,,3`,
		"log_dir_total_bytes,3,/broken,,,",
		"log_dir_used_bytes,3,/broken,,,",
		"log_dir_usable_bytes,3,/broken,,,",
		"log_dir_kafka_bytes,3,/broken,,,",
		"topics,,,,,2",
		"partitions,,,,,4",
		"partitions_under_replicated,,,,,1",
		"partitions_below_min_isr,,,,,",
		"partitions_offline,,,,,0",
		"topic_size_bytes,,,orders,,7",
		"topic_end_offset,,,orders,,9007199254740993",
		"topic_end_offset,,,failing,,",
		"",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestMetricsToCSVLeavesAFailedPartitionSectionEmpty(t *testing.T) {
	csv := MetricsToCSV(decode(t, strings.Replace(base, `"partitions": {"topics": 2`, `"partitions": {"error": "timeout", "topics": 2`, 1)+"}"))
	if !strings.Contains(csv, "topics,,,,,\r\npartitions,,,,,\r\n") {
		t.Fatalf("%s", csv)
	}
}

func TestMetricsToCSVM17Sections(t *testing.T) {
	csv := MetricsToCSV(decode(t, base+`,
  "quorum": {"leaderId": 1, "leaderEpoch": 3, "highWatermark": 894587, "replicas": [
    {"id": 1, "role": "voter", "logEndOffset": 894587, "lag": 0, "lastFetch": null}, {"id": 4, "role": "observer", "logEndOffset": 5, "lag": null, "lastFetch": null}]},
  "brokers": {"brokers": [{"broker": 1, "leaders": 39, "replicas": 40, "nonPreferredLeaders": 2}]},
  "consumerGroups": {"states": [{"state": "Stable", "count": 2}], "groups": [
    {"group": "orders", "state": "Stable", "members": 2, "lag": "9007199254740993", "maxPartitionLag": "9007199254740990", "unknownPartitions": 0},
    {"group": "a,b", "state": "Stable", "members": 0, "lag": "3", "maxPartitionLag": "3", "unknownPartitions": 1}]},
  "connect": {"connectors": [{"state": "RUNNING", "count": 4}], "tasks": [{"state": "FAILED", "count": 1}], "failures": []},
  "backups": {"backups": [
    {"topic": "orders", "state": "RUNNING", "storageListed": true, "objects": 10, "storedBytes": 1900, "lastWrite": "2026-11-11T03:30:30Z", "restoring": false},
    {"topic": "paused", "state": "PAUSED", "storageListed": false, "objects": null, "storedBytes": null, "lastWrite": null, "restoring": true}]}}`))
	for _, line := range []string{
		"quorum_leader,,,,,1", "quorum_high_watermark,,,,,894587", "quorum_replica_lag,1,,,voter,0", "quorum_replica_lag,4,,,observer,",
		"broker_leaders,1,,,,39", "broker_replicas,1,,,,40", "broker_non_preferred_leaders,1,,,,2",
		"consumer_groups,,,,Stable,2", "consumer_group_members,,,,orders,2", "consumer_group_lag,,,,orders,9007199254740993",
		"consumer_group_max_partition_lag,,,,orders,9007199254740990", `consumer_group_lag,,,,"a,b",`, `consumer_group_unknown_lag_partitions,,,,"a,b",1`,
		"connect_connectors,,,,RUNNING,4", "connect_tasks,,,,FAILED,1",
		"backup_up,,,orders,RUNNING,1", "backup_stored_bytes,,,orders,,1900", "backup_last_write,,,orders,,2026-11-11T03:30:30Z",
		"backup_up,,,paused,PAUSED,0", "backup_restoring,,,paused,,1", "backup_stored_bytes,,,paused,,",
	} {
		if !strings.Contains(csv, line+"\r\n") {
			t.Errorf("missing %q", line)
		}
	}
	notConfigured := MetricsToCSV(decode(t, base+`, "connect": {"notConfigured": true, "connectors": [], "tasks": [], "failures": []}, "backups": {"notConfigured": true, "backups": []}}`))
	if strings.Contains(notConfigured, "connect_") || strings.Contains(notConfigured, "backup_") {
		t.Fatal("a section that is not configured wrote rows")
	}
	failedQuorum := MetricsToCSV(decode(t, base+`, "quorum": {"error": "denied", "leaderId": null, "leaderEpoch": 0, "highWatermark": 0, "replicas": []}}`))
	if !strings.Contains(failedQuorum, "quorum_leader,,,,,\r\nquorum_high_watermark,,,,,\r\n") {
		t.Fatalf("%s", failedQuorum)
	}
}
